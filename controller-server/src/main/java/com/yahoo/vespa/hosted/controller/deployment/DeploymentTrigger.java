// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.deployment;

import com.yahoo.component.Version;
import com.yahoo.config.application.api.DeploymentSpec;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.Environment;
import com.yahoo.config.provision.SystemName;
import com.yahoo.config.provision.Zone;
import com.yahoo.vespa.curator.Lock;
import com.yahoo.vespa.hosted.controller.Application;
import com.yahoo.vespa.hosted.controller.ApplicationController;
import com.yahoo.vespa.hosted.controller.Controller;
import com.yahoo.vespa.hosted.controller.LockedApplication;
import com.yahoo.vespa.hosted.controller.application.ApplicationList;
import com.yahoo.vespa.hosted.controller.application.ApplicationRevision;
import com.yahoo.vespa.hosted.controller.application.Change;
import com.yahoo.vespa.hosted.controller.application.Deployment;
import com.yahoo.vespa.hosted.controller.application.DeploymentJobs;
import com.yahoo.vespa.hosted.controller.application.DeploymentJobs.JobError;
import com.yahoo.vespa.hosted.controller.application.DeploymentJobs.JobReport;
import com.yahoo.vespa.hosted.controller.application.DeploymentJobs.JobType;
import com.yahoo.vespa.hosted.controller.application.JobList;
import com.yahoo.vespa.hosted.controller.application.JobStatus;
import com.yahoo.vespa.hosted.controller.persistence.CuratorDb;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Responsible for scheduling deployment jobs in a build system and keeping
 * Application.deploying() in sync with what is scheduled.
 * 
 * This class is multithread safe.
 * 
 * @author bratseth
 * @author mpolden
 */
public class DeploymentTrigger {

    /** The max duration a job may run before we consider it dead/hanging */
    private final Duration jobTimeout;

    private final static Logger log = Logger.getLogger(DeploymentTrigger.class.getName());

    private final Controller controller;
    private final Clock clock;
    private final BuildSystem buildSystem;
    private final DeploymentOrder order;

    public DeploymentTrigger(Controller controller, CuratorDb curator, Clock clock) {
        Objects.requireNonNull(controller,"controller cannot be null");
        Objects.requireNonNull(curator,"curator cannot be null");
        Objects.requireNonNull(clock,"clock cannot be null");
        this.controller = controller;
        this.clock = clock;
        this.buildSystem = new PolledBuildSystem(controller, curator);
        this.order = new DeploymentOrder(controller);
        this.jobTimeout = controller.system().equals(SystemName.main) ? Duration.ofHours(12) : Duration.ofHours(1);
    }
    
    /** Returns the time in the past before which jobs are at this moment considered unresponsive */
    public Instant jobTimeoutLimit() { return clock.instant().minus(jobTimeout); }
    
    //--- Start of methods which triggers deployment jobs -------------------------

    /** 
     * Called each time a job completes (successfully or not) to cause triggering of one or more follow-up jobs
     * (which may possibly the same job once over).
     * 
     * @param report information about the job that just completed
     */
    public void triggerFromCompletion(JobReport report) {
        try (Lock lock = applications().lock(report.applicationId())) {
            LockedApplication application = applications().require(report.applicationId(), lock);
            application = application.withJobCompletion(report, clock.instant(), controller);

            // Handle successful starting and ending
            if (report.success()) {
                if (order.givesNewRevision(report.jobType())) {
                    if (acceptNewRevisionNow(application)) {
                        // Set this as the change we are doing, unless we are already pushing a platform change
                        if ( ! ( application.deploying().isPresent() &&
                                 (application.deploying().get() instanceof Change.VersionChange)))
                            application = application.withDeploying(Optional.of(Change.ApplicationChange.unknown()));
                    }
                    else { // postpone
                        applications().store(application.withOutstandingChange(true));
                        return;
                    }
                }
                else if (deploymentComplete(application)) {
                    // change completed
                    application = application.withDeploying(Optional.empty());
                }
            }

            // Trigger next
            if (report.success())
                application = trigger(order.nextAfter(report.jobType(), application), application,
                                      report.jobType().jobName() + " completed");
            else if (isCapacityConstrained(report.jobType()) && shouldRetryOnOutOfCapacity(application, report.jobType()))
                application = trigger(report.jobType(), application, true,
                                      "Retrying on out of capacity");
            else if (shouldRetryNow(application, report.jobType()))
                application = trigger(report.jobType(), application, false,
                                      "Immediate retry on failure");

            applications().store(application);
        }
    }

    /** Returns whether all production zones listed in deployment spec last were successful on the currently deploying change. */
    private boolean deploymentComplete(LockedApplication application) {
        if ( ! application.deploying().isPresent()) return true;
        return order.jobsFrom(application.deploymentSpec()).stream()
                .filter(JobType::isProduction)
                .allMatch(jobType -> application.deploymentJobs().isSuccessful(application.deploying().get(), jobType));
    }

    /**
     * Find jobs that can and should run but are currently not.
     */
    public void triggerReadyJobs() {
        ApplicationList applications = ApplicationList.from(applications().asList());
        applications = applications.notPullRequest();
        for (Application application : applications.asList()) {
            try (Lock lock = applications().lock(application.id())) {
                Optional<LockedApplication> lockedApplication = controller.applications().get(application.id(), lock);
                if ( ! lockedApplication.isPresent()) continue; // application removed
                triggerReadyJobs(lockedApplication.get());
            }
        }
    }

    /** Find the next step to trigger if any, and triggers it */
    private void triggerReadyJobs(LockedApplication application) {
        if ( ! application.deploying().isPresent()) return;
        List<JobType> jobs =  order.jobsFrom(application.deploymentSpec());

        // Should the first step be triggered?
        // TODO: How can the first job not be systemTest (second ccondition)?
        if ( ! jobs.isEmpty() && jobs.get(0).equals(JobType.systemTest) &&
             application.deploying().get() instanceof Change.VersionChange) {
            Version target = ((Change.VersionChange)application.deploying().get()).version();
            JobStatus jobStatus = application.deploymentJobs().jobStatus().get(JobType.systemTest);
            if (jobStatus == null || ! jobStatus.lastTriggered().isPresent() 
                || ! jobStatus.lastTriggered().get().version().equals(target)) {
                application = trigger(JobType.systemTest, application, false, "Upgrade to " + target);
                controller.applications().store(application);
            }
        }

        // Find next steps to trigger based on the state of the previous step
        for (JobType jobType : jobs) {
            JobStatus jobStatus = application.deploymentJobs().jobStatus().get(jobType);
            if (jobStatus == null) continue; // job has never run
            if (jobStatus.isRunning(jobTimeoutLimit())) continue;

            // Collect the subset of next jobs which have not run with the last changes
            List<JobType> nextToTrigger = new ArrayList<>();
            for (JobType nextJobType : order.nextAfter(jobType, application)) {
                JobStatus nextStatus = application.deploymentJobs().jobStatus().get(nextJobType);
                if (changesAvailable(application, jobStatus, nextStatus))
                    nextToTrigger.add(nextJobType);
            }
            // Trigger them in parallel
            application = trigger(nextToTrigger, application, "Available change in " + jobType.jobName());
            controller.applications().store(application);
        }
    }

    /**
     * Returns true if the previous job has completed successfully with a revision and/or version which is
     * newer (different) than the one last completed successfully in next
     */
    private boolean changesAvailable(Application application, JobStatus previous, JobStatus next) {
        if ( ! application.deploying().isPresent()) return false;
        Change change = application.deploying().get();

        if ( ! previous.lastSuccess().isPresent() && 
             ! productionUpgradeHasSucceededFor(previous, change)) return false;

        if (change instanceof Change.VersionChange) {
            Version targetVersion = ((Change.VersionChange)change).version();
            if ( ! (targetVersion.equals(previous.lastSuccess().get().version())) )
                return false; // version is outdated
            if (isOnNewerVersionInProductionThan(targetVersion, application, next.type()))
                return false; // Don't downgrade
        }

        if (next == null) return true;
        if ( ! next.lastSuccess().isPresent()) return true;

        JobStatus.JobRun previousSuccess = previous.lastSuccess().get();
        JobStatus.JobRun nextSuccess = next.lastSuccess().get();
        if (previousSuccess.revision().isPresent() &&  ! previousSuccess.revision().get().equals(nextSuccess.revision().get()))
            return true;
        if ( ! previousSuccess.version().equals(nextSuccess.version()))
            return true;
        return false;
    }
    
    /**
     * Called periodically to cause triggering of jobs in the background
     */
    public void triggerFailing(ApplicationId applicationId) {
        try (Lock lock = applications().lock(applicationId)) {
            LockedApplication application = applications().require(applicationId, lock);
            if ( ! application.deploying().isPresent()) return; // No ongoing change, no need to retry

            // Retry first failing job
            for (JobType jobType : order.jobsFrom(application.deploymentSpec())) {
                JobStatus jobStatus = application.deploymentJobs().jobStatus().get(jobType);
                if (isFailing(application.deploying().get(), jobStatus)) {
                    if (shouldRetryNow(jobStatus)) {
                        application = trigger(jobType, application, false, "Retrying failing job");
                        applications().store(application);
                    }
                    break;
                }
            }

            // Retry dead job
            Optional<JobStatus> firstDeadJob = firstDeadJob(application.deploymentJobs());
            if (firstDeadJob.isPresent()) {
                application = trigger(firstDeadJob.get().type(), application, false, "Retrying dead job");
                applications().store(application);
            }
        }
    }

    /** Triggers jobs that have been delayed according to deployment spec */
    public void triggerDelayed() {
        for (Application application : applications().asList()) {
            if ( ! application.deploying().isPresent() ) continue;
            if (application.deploymentJobs().hasFailures()) continue;
            if (application.deploymentJobs().isRunning(controller.applications().deploymentTrigger().jobTimeoutLimit())) continue;
            if (application.deploymentSpec().steps().stream().noneMatch(step -> step instanceof DeploymentSpec.Delay)) {
                continue; // Application does not have any delayed deployments
            }

            Optional<JobStatus> lastSuccessfulJob = application.deploymentJobs().jobStatus().values()
                    .stream()
                    .filter(j -> j.lastSuccess().isPresent())
                    .sorted(Comparator.<JobStatus, Instant>comparing(j -> j.lastSuccess().get().at()).reversed())
                    .findFirst();
            if ( ! lastSuccessfulJob.isPresent() ) continue;

            // Trigger next
            try (Lock lock = applications().lock(application.id())) {
                LockedApplication lockedApplication = applications().require(application.id(), lock);
                lockedApplication = trigger(order.nextAfter(lastSuccessfulJob.get().type(), lockedApplication),
                                            lockedApplication, "Resuming delayed deployment");
                applications().store(lockedApplication);
            }
        }
    }
    
    /**
     * Triggers a change of this application
     * 
     * @param applicationId the application to trigger
     * @throws IllegalArgumentException if this application already have an ongoing change
     */
    public void triggerChange(ApplicationId applicationId, Change change) {
        try (Lock lock = applications().lock(applicationId)) {
            LockedApplication application = applications().require(applicationId, lock);
            if (application.deploying().isPresent()  && ! application.deploymentJobs().hasFailures())
                throw new IllegalArgumentException("Could not start " + change + " on " + application + ": " + 
                                                   application.deploying().get() + " is already in progress");
            application = application.withDeploying(Optional.of(change));
            if (change instanceof Change.ApplicationChange)
                application = application.withOutstandingChange(false);
            application = trigger(JobType.systemTest, application, false, "Deploying " + change);
            applications().store(application);
        }
    }

    /**
     * Cancels any ongoing upgrade of the given application
     *
     * @param applicationId the application to trigger
     */
    public void cancelChange(ApplicationId applicationId) {
        try (Lock lock = applications().lock(applicationId)) {
            LockedApplication application = applications().require(applicationId, lock);
            buildSystem.removeJobs(application.id());
            application = application.withDeploying(Optional.empty());
            applications().store(application);
        }
    }

    //--- End of methods which triggers deployment jobs ----------------------------

    private ApplicationController applications() { return controller.applications(); }

    /** Returns whether a job is failing for the current change in the given application */
    private boolean isFailing(Change change, JobStatus status) {
        return       status != null
                && ! status.isSuccess()
                &&   status.lastCompleted().get().lastCompletedWas(change);
    }

    private boolean isCapacityConstrained(JobType jobType) {
        return jobType == JobType.stagingTest || jobType == JobType.systemTest;
    }

    /** Returns the first job that has been running for more than the given timeout */
    private Optional<JobStatus> firstDeadJob(DeploymentJobs jobs) {
        Optional<JobStatus> oldestRunningJob = jobs.jobStatus().values().stream()
                .filter(job -> job.isRunning(Instant.ofEpochMilli(0)))
                .sorted(Comparator.comparing(status -> status.lastTriggered().get().at()))
                .findFirst();
        return oldestRunningJob.filter(job -> job.lastTriggered().get().at().isBefore(jobTimeoutLimit()));
    }

    /** Decide whether the job should be triggered by the periodic trigger */
    private boolean shouldRetryNow(JobStatus job) {
        if (job.isSuccess()) return false;
        if (job.isRunning(jobTimeoutLimit())) return false;

        // Retry after 10% of the time since it started failing
        Duration aTenthOfFailTime = Duration.ofMillis( (clock.millis() - job.firstFailing().get().at().toEpochMilli()) / 10);
        if (job.lastCompleted().get().at().isBefore(clock.instant().minus(aTenthOfFailTime))) return true;

        // ... or retry anyway if we haven't tried in 4 hours
        if (job.lastCompleted().get().at().isBefore(clock.instant().minus(Duration.ofHours(4)))) return true;

        return false;
    }
    
    /** Retry immediately only if this job just started failing. Otherwise retry periodically */
    private boolean shouldRetryNow(Application application, JobType jobType) {
        JobStatus jobStatus = application.deploymentJobs().jobStatus().get(jobType);
        return (jobStatus != null && jobStatus.firstFailing().get().at().isAfter(clock.instant().minus(Duration.ofSeconds(10))));
    }

    /** Decide whether to retry due to capacity restrictions */
    private boolean shouldRetryOnOutOfCapacity(Application application, JobType jobType) {
        Optional<JobError> outOfCapacityError = Optional.ofNullable(application.deploymentJobs().jobStatus().get(jobType))
                .flatMap(JobStatus::jobError)
                .filter(e -> e.equals(JobError.outOfCapacity));

        if ( ! outOfCapacityError.isPresent()) return false;

        // Retry the job if it failed recently
        return application.deploymentJobs().jobStatus().get(jobType).firstFailing().get().at()
                .isAfter(clock.instant().minus(Duration.ofMinutes(15)));
    }

    /** Returns whether the given job type should be triggered according to deployment spec */
    private boolean deploysTo(Application application, JobType jobType) {
        Optional<Zone> zone = jobType.zone(controller.system());
        if (zone.isPresent() && jobType.isProduction()) {
            // Skip triggering of jobs for zones where the application should not be deployed
            if ( ! application.deploymentSpec().includes(jobType.environment(), Optional.of(zone.get().region()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Trigger a job for an application 
     *
     * @param jobType the type of the job to trigger, or null to trigger nothing
     * @param application the application to trigger the job for
     * @param first whether to put the job at the front of the build system queue (or the back)
     * @param reason describes why the job is triggered
     * @return the application in the triggered state, which *must* be stored by the caller
     */
    private LockedApplication trigger(JobType jobType, LockedApplication application, boolean first, String reason) {
        if (isRunningProductionJob(application)) return application;
        return triggerAllowParallel(jobType, application, first, false, reason);
    }

    private LockedApplication trigger(List<JobType> jobs, LockedApplication application, String reason) {
        if (isRunningProductionJob(application)) return application;
        for (JobType job : jobs)
            application = triggerAllowParallel(job, application, false, false, reason);
        return application;
    }

    /**
     * Trigger a job for an application, if allowed
     * 
     * @param jobType the type of the job to trigger, or null to trigger nothing
     * @param application the application to trigger the job for
     * @param first whether to trigger the job before other jobs
     * @param force true to disable checks which should normally prevent this triggering from happening
     * @param reason describes why the job is triggered
     * @return the application in the triggered state, if actually triggered. This *must* be stored by the caller
     */
    public LockedApplication triggerAllowParallel(JobType jobType, LockedApplication application,
                                                  boolean first, boolean force, String reason) {
        if (jobType == null) return application; // we are passed null when the last job has been reached
        // Never allow untested changes to go through
        // Note that this may happen because a new change catches up and prevents an older one from continuing
        if ( ! application.deploymentJobs().isDeployableTo(jobType.environment(), application.deploying())) {
            log.warning(String.format("Want to trigger %s for %s with reason %s, but change is untested", jobType,
                                      application, reason));
            return application;
        }

        if ( ! force && ! allowedTriggering(jobType, application)) return application;
        log.info(String.format("Triggering %s for %s, %s: %s", jobType, application,
                               application.deploying().map(d -> "deploying " + d).orElse("restarted deployment"),
                               reason));
        buildSystem.addJob(application.id(), jobType, first);
        return application.withJobTriggering(jobType, application.deploying(), reason, clock.instant(), controller);
    }

    /** Returns true if the given proposed job triggering should be effected */
    private boolean allowedTriggering(JobType jobType, LockedApplication application) {
        // Note: We could make a more fine-grained and more correct determination about whether to block 
        //       by instead basing the decision on what is currently deployed in the zone. However,
        //       this leads to some additional corner cases, and the possibility of blocking an application
        //       fix to a version upgrade, so not doing it now
        if (jobType.isProduction() && application.deployingBlocked(clock.instant())) return false;
        if (application.deploymentJobs().isRunning(jobType, jobTimeoutLimit())) return false;
        if  ( ! deploysTo(application, jobType)) return false;
        // Ignore applications that are not associated with a project
        if ( ! application.deploymentJobs().projectId().isPresent()) return false;
        if (application.deploying().isPresent() && application.deploying().get() instanceof Change.VersionChange) {
            Version targetVersion = ((Change.VersionChange)application.deploying().get()).version();
            if (isOnNewerVersionInProductionThan(targetVersion, application, jobType)) return false; // Don't downgrade
        }
        
        return true;
    }
    
    private boolean isRunningProductionJob(Application application) {
        return JobList.from(application)
                .production()
                .running(jobTimeoutLimit())
                .anyMatch();
    }

    /**
     * When upgrading it is ok to trigger the next job even if the previous failed if the previous has earlier succeeded
     * on the version we are currently upgrading to
     */
    private boolean productionUpgradeHasSucceededFor(JobStatus jobStatus, Change change) {
        if ( ! (change instanceof Change.VersionChange) ) return false;
        if ( ! isProduction(jobStatus.type())) return false;
        Optional<JobStatus.JobRun> lastSuccess = jobStatus.lastSuccess();
        if ( ! lastSuccess.isPresent()) return false;
        return lastSuccess.get().version().equals(((Change.VersionChange)change).version());
    }

    /** 
     * Returns whether the current deployed version in the zone given by the job
     * is newer than the given version. This may be the case even if the production job
     * in question failed, if the failure happens after deployment.
     * In that case we should never deploy an earlier version as that may potentially
     * downgrade production nodes which we are not guaranteed to support.
     */
    private boolean isOnNewerVersionInProductionThan(Version version, Application application, JobType job) {
        if ( ! isProduction(job)) return false;
        Optional<Zone> zone = job.zone(controller.system());
        if ( ! zone.isPresent()) return false;
        Deployment existingDeployment = application.deployments().get(zone.get());
        if (existingDeployment == null) return false;
        return existingDeployment.version().isAfter(version);
    }
    
    private boolean isProduction(JobType job) {
        Optional<Zone> zone = job.zone(controller.system());
        if ( ! zone.isPresent()) return false; // arbitrary
        return zone.get().environment() == Environment.prod;
    }
    
    private boolean acceptNewRevisionNow(LockedApplication application) {
        if ( ! application.deploying().isPresent()) return true;
        if ( application.deploying().get() instanceof Change.ApplicationChange) return true; // more changes are ok

        if ( application.deploymentJobs().hasFailures()) return true; // allow changes to fix upgrade problems
        if ( application.isBlocked(clock.instant())) return true; // allow testing changes while upgrade blocked (debatable)
        // Otherwise, the application is currently upgrading, without failures, and we should wait with the revision.
        return false;
    }
    
    public BuildSystem buildSystem() { return buildSystem; }

    public DeploymentOrder deploymentOrder() { return order; }

    /**
     * Loop through the steps of the deployment spec for the given application and propagate state changes as prescribed.
     *
     * The intial state is determined by whether there are revision or version changes to roll out.
     * For each following step, as given by the deployment spec, check whether the previous state is valid,
     * different from the state of the jobs in the step and with differences that are currently permitted;
     * if this is all true, determine the reason for triggering the current job, if any. If there is a reason
     * to trigger, and if it is allowed to trigger that job right now, then do so.
     * Finally, if the last step succeeded with a valid state, mark the changes of this state as complete.
     */
    // This method should be called both by the upgrade maintainer, and on job completion.
    private void triggerJobsFor(LockedApplication application) {
        State state = State.inital(application);
        for (DeploymentSpec.Step step : application.deploymentSpec().steps()) {
            // For the jobs of this step, trigger what should be triggered based on the state of the previous step:
            List<JobType> stepJobs = step.zones().stream().map(this::jobFor).collect(Collectors.toList());
            application.deploymentJobs().jobStatus();
            for (JobType jobType : stepJobs) {
                JobStatus jobStatus = application.deploymentJobs().jobStatus().get(jobType);
                if (state.isSimilarTo(State.of(jobStatus))) continue; // If the two states are the same, do nothing.

                State target = state;
                if ( ! application.deploymentSpec().canChangeRevisionAt(clock.instant())) target = target.withoutRevision();
                if ( ! application.deploymentSpec().canUpgradeAt(clock.instant())) target = target.withoutVersion();
                // Also peel away versions that are no longer desired: no longer the deploying(), or, perhaps, no longer have high enough confidence?
                if (target.isInvalid()) continue; // If all possible changes were prohibited, do nothing.

                Optional<String> reason = reasonForTriggering(jobStatus);
                if ( ! reason.isPresent()) continue; // If we had no reason to trigger this job now: don't!

                if ( ! canTriggerNow(jobType, target, application, stepJobs)) continue; // Go somewhere else to force.

                buildSystem.addJob(application.id(), jobType, reason.get().equals("Retrying immediately, as the job failed on capacity constraints."));
                application = application.withJobTriggering(jobType, application.deploying(), reason.get(), clock.instant(), controller);

            }
            // Finally, find the exit state of this step, to propagate further:
            state = JobList.from(application).types(stepJobs).commonState()
                    // A bit convoluted, perhaps; if empty stepJobs, this is a Delay, and we just delay the input state.
                    .orElse(state.delay(((DeploymentSpec.Delay) step).duration(), clock.instant()));
        }
        if (state.version().isPresent()) application = application.withDeploying(Optional.empty()); // TODO: Change this, obviously.
        controller.applications().store(application);
    }

    private JobType jobFor(DeploymentSpec.DeclaredZone zone) {
        return JobType.from(controller.system(), zone.environment(), zone.region().orElse(null))
                .orElseThrow(() -> new IllegalArgumentException("Invalid zone " + zone));
    }

    private Optional<String> reasonForTriggering(JobStatus jobStatus) {
        if (jobStatus == null) return Optional.of("Job became available for triggering for the first time.");
        if (jobStatus.isRunning(jobTimeoutLimit())) return Optional.empty();
        if (jobStatus.isSuccess()) return Optional.of("A new change passed successfully through the upstream step.");
        if (jobStatus.jobError().filter(JobError.outOfCapacity::equals).isPresent()) return Optional.of("Retrying immediately, as the job failed on capacity constraints.");
        if (jobStatus.firstFailing().get().at().isAfter(clock.instant().minus(Duration.ofSeconds(10)))) return Optional.of("Immediate retry, as " + jobStatus.type() + " just started failing.");
        if (   jobStatus.lastCompleted().get().at().isBefore(clock.instant().minus(Duration.between(jobStatus.firstFailing().get().at(), clock.instant()).dividedBy(10)))
               || jobStatus.lastCompleted().get().at().isBefore(clock.instant().minus(Duration.ofHours(4)))) return Optional.of("Delayed retry, as " + jobStatus.type() + " hasn't been retried for a while.");
        return Optional.empty();
    }

    private boolean canTriggerNow(JobType jobType, State target, Application application, List<JobType> concurrentJobs) {
        if ( ! application.deploymentJobs().projectId().isPresent()) return false;
        if (target.version().isPresent() && jobType.isProduction() && isOnNewerVersionThan(target.version().get(), jobType, application)) return false;
        if ( ! concurrentJobs.containsAll(JobList.from(application).running(jobTimeoutLimit()).production().asList())) return false;
        return true;
    }

    private boolean isOnNewerVersionThan(Version version, JobType jobType, Application application) {
        return jobType.zone(controller.system())
                .map(zone -> application.deployments().get(zone))
                .filter(deployment -> deployment.version().isAfter(version))
                .isPresent();
    }

    /**
     * Contains information about the last successful state of a Step.
     *
     * Two states can be merged by similarity, and delayed by a given delay. If the resulting state
     * is invalid, it means some next step does not (yet) have a new state to upgrade to;
     * otherwise, the resulting state provides target version and revision for some next step.
     */
    public static class State {

        public static final ApplicationRevision unknownRevision = ApplicationRevision.from("");
        public static final State invalid = new State(null, null, null);

        private final Version version;
        private final ApplicationRevision revision;
        private final Instant completion;

        private State(Version version, ApplicationRevision revision, Instant completion) {
            this.version = version;
            this.revision = revision;
            this.completion = completion;
        }

        public static State of(JobStatus jobStatus) {
            if (jobStatus == null || ! jobStatus.lastSuccess().isPresent()) return State.invalid;
            return new State(jobStatus.lastSuccess().get().version(),
                             jobStatus.lastSuccess().get().revision().get(), // TODO: Make revision non-optional.
                             jobStatus.lastSuccess().get().at());
        }

        public static State inital(Application application) {
            // version    <-- application is upgrading ? target version : null
            // revision   <-- application has new revision ready to go and is not successfully upgrading ? unknownRevision : null
            // completion <-- null
            return new State(null, null, null);
        }

        /** Returns the state with the later completion if they are similar, and invalid otherwise. */
        public State merge(State other) {
            return ! isInvalid() && isSimilarTo(other)
                    ? completion.isAfter(other.completion) ? this : other
                    : invalid;
        }

        /** Returns a state with completion delayed by the given delay, or invalid if this is before the given now. */
        public State delay(Duration delay, Instant now) {
            return (isInvalid() || completion.plus(delay).isAfter(now))
                    ? invalid
                    : new State(version, revision, completion.plus(delay));
        }

        /** Returns whether the two states agree on version and revision. */
        public boolean isSimilarTo(State other) {
            return version().equals(other.version()) && revision().equals(other.revision());
        }

        /** Returns whether this state provides a valid upgrade target. */
        public boolean isInvalid() {
            return completion == null || (version == null && revision == null);
        }

        public Optional<Version> version() {
            return Optional.ofNullable(version);
        }

        public Optional<ApplicationRevision> revision() {
            return Optional.ofNullable(revision);
        }

        public State withoutVersion() {
            return new State(null, revision, completion);
        }

        public State withoutRevision() {
            return new State(version, null, completion);
        }

    }

}
