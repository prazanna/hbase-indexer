package com.ngdata.hbaseindexer.master;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.ngdata.hbaseindexer.model.api.ActiveBatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfoBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerNotFoundException;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerModelEvent;
import com.ngdata.hbaseindexer.model.api.IndexerModelEventType;
import com.ngdata.hbaseindexer.model.api.IndexerModelListener;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.hbaseindexer.util.zookeeper.LeaderElection;
import com.ngdata.hbaseindexer.util.zookeeper.LeaderElectionCallback;
import com.ngdata.hbaseindexer.util.zookeeper.LeaderElectionSetupException;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.zookeeper.KeeperException;

import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.BatchIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.IncrementalIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerModelEventType.*;

/**
 * The indexer master is active on only one Lily node and is responsible for things such as launching batch indexing
 * jobs, assigning or removing message queue subscriptions, and the like.
 */
public class IndexerMaster {
    private final ZooKeeperItf zk;

    private final WriteableIndexerModel indexerModel;

    private final Configuration mapReduceConf;

    private final Configuration hbaseConf;

    private final Configuration mapReduceJobConf;

    private final String zkConnectString;

    private final int zkSessionTimeout;

    // TODO
//    private final SolrClientConfig solrClientConfig;

    private final String hostName;

    private LeaderElection leaderElection;

    private IndexerModelListener listener = new MyListener();

    private JobStatusWatcher jobStatusWatcher = new JobStatusWatcher();

    private EventWorker eventWorker = new EventWorker();

    private JobClient jobClient;

    private final Log log = LogFactory.getLog(getClass());

    private byte[] fullTableScanConf;

    private SepModel sepModel;

    public IndexerMaster(ZooKeeperItf zk, WriteableIndexerModel indexerModel,
            Configuration mapReduceConf, Configuration mapReduceJobConf, Configuration hbaseConf,
            String zkConnectString, int zkSessionTimeout, SepModel sepModel, String hostName) {

        this.zk = zk;
        this.indexerModel = indexerModel;
        this.mapReduceConf = mapReduceConf;
        this.mapReduceJobConf = mapReduceJobConf;
        this.hbaseConf = hbaseConf;
        this.zkConnectString = zkConnectString;
        this.zkSessionTimeout = zkSessionTimeout;
        this.hostName = hostName;
        this.sepModel = sepModel;
    }

    @PostConstruct
    public void start() throws LeaderElectionSetupException, IOException, InterruptedException, KeeperException {
        leaderElection = new LeaderElection(zk, "Indexer Master", "/lily/indexer/masters",
                new MyLeaderElectionCallback());

        // TODO
//        InputStream is = null;
//        try {
//            is = IndexerMaster.class.getResourceAsStream("full-table-scan-config.json");
//            this.fullTableScanConf = IOUtils.toByteArray(is);
//        } finally {
//            IOUtils.closeQuietly(is);
//        }

    }

    @PreDestroy
    public void stop() {
        try {
            if (leaderElection != null)
                leaderElection.stop();
        } catch (InterruptedException e) {
            log.info("Interrupted while shutting down leader election.");
        }

        Closer.close(jobClient);
    }

    private synchronized JobClient getJobClient() throws IOException {
        if (jobClient == null) {
            jobClient = new JobClient(new JobConf(mapReduceConf));
        }
        return jobClient;
    }

    private class MyLeaderElectionCallback implements LeaderElectionCallback {
        @Override
        public void activateAsLeader() throws Exception {
            log.info("Starting up as indexer master.");

            // Start these processes, but it is not until we have registered our model listener
            // that these will receive work.
            eventWorker.start();
            jobStatusWatcher.start();

            Collection<IndexerDefinition> indexers = indexerModel.getIndexers(listener);

            // Rather than performing any work that might to be done for the indexers here,
            // we push out fake events. This way there's only one place where these actions
            // need to be performed.
            for (IndexerDefinition index : indexers) {
                eventWorker.putEvent(new IndexerModelEvent(INDEXER_UPDATED, index.getName()));
            }

            log.info("Startup as indexer master successful.");
        }

        @Override
        public void deactivateAsLeader() throws Exception {
            log.info("Shutting down as indexer master.");

            indexerModel.unregisterListener(listener);

            // Argument false for shutdown: we do not interrupt the event worker thread: if there
            // was something running there that is blocked until the ZK connection comes back up
            // we want it to finish (e.g. a lock taken that should be released again)
            eventWorker.shutdown(false);
            jobStatusWatcher.shutdown(false);

            log.info("Shutdown as indexer master successful.");
        }
    }

    private boolean needsSubscriptionIdAssigned(IndexerDefinition indexer) {
        return !indexer.getLifecycleState().isDeleteState() &&
                indexer.getIncrementalIndexingState() != IncrementalIndexingState.DO_NOT_SUBSCRIBE
                && indexer.getSubscriptionId() == null;
    }

    private boolean needsSubscriptionIdUnassigned(IndexerDefinition indexer) {
        return indexer.getIncrementalIndexingState() == IncrementalIndexingState.DO_NOT_SUBSCRIBE
                && indexer.getSubscriptionId() != null;
    }

    private boolean needsBatchBuildStart(IndexerDefinition indexer) {
        return !indexer.getLifecycleState().isDeleteState() &&
                indexer.getBatchIndexingState() == BatchIndexingState.BUILD_REQUESTED &&
                indexer.getActiveBatchBuildInfo() == null;
    }

    private void assignSubscription(String indexerName) {
        try {
            String lock = indexerModel.lockIndexer(indexerName);
            try {
                // Read current situation of record and assure it is still actual
                IndexerDefinition indexer = indexerModel.getFreshIndexer(indexerName);
                if (needsSubscriptionIdAssigned(indexer)) {
                    // We assume we are the only process which creates subscriptions which begin with the
                    // prefix "IndexUpdater:". This way we are sure there are no naming conflicts or conflicts
                    // due to concurrent operations (e.g. someone deleting this subscription right after we
                    // created it).
                    String subscriptionId = subscriptionId(indexer.getName());
                    sepModel.addSubscription(subscriptionId);
                    indexer = new IndexerDefinitionBuilder().startFrom(indexer).subscriptionId(subscriptionId).build();
                    indexerModel.updateIndexerInternal(indexer);
                    log.info("Assigned subscription ID '" + subscriptionId + "' to indexer '" + indexerName + "'");
                }
            } finally {
                indexerModel.unlockIndexer(lock);
            }
        } catch (Throwable t) {
            log.error("Error trying to assign a subscription to index " + indexerName, t);
        }
    }

    private void unassignSubscription(String indexerName) {
        try {
            String lock = indexerModel.lockIndexer(indexerName);
            try {
                // Read current situation of record and assure it is still actual
                IndexerDefinition indexer = indexerModel.getFreshIndexer(indexerName);
                if (needsSubscriptionIdUnassigned(indexer)) {
                    sepModel.removeSubscription(indexer.getSubscriptionId());
                    log.info("Deleted queue subscription for indexer " + indexerName);
                    indexer = new IndexerDefinitionBuilder().startFrom(indexer).subscriptionId(null).build();
                    indexerModel.updateIndexerInternal(indexer);
                }
            } finally {
                indexerModel.unlockIndexer(lock);
            }
        } catch (Throwable t) {
            log.error("Error trying to delete the subscription for indexer " + indexerName, t);
        }
    }

    private String subscriptionId(String indexerName) {
        return "IndexUpdater_" + indexerName;
    }

    private void startFullIndexBuild(String indexerName) {
        // TODO
//        try {
//            String lock = indexerModel.lockIndex(indexName);
//            try {
//                // Read current situation of record and assure it is still actual
//                IndexDefinition index = indexerModel.getMutableIndex(indexName);
//                byte[] batchIndexConfiguration = getBatchIndexConfiguration(index);
//                index.setBatchIndexConfiguration(null);
//                List<String> batchTables = getBatchTables(index);
//                index.setBatchTables(null);
//                if (needsBatchBuildStart(index)) {
//                    Job job = null;
//                    boolean jobStarted;
//                    try {
//                        job = BatchIndexBuilder.startBatchBuildJob(index, mapReduceJobConf, hbaseConf,
//                                repositoryManager, zkConnectString, zkSessionTimeout, solrClientConfig,
//                                batchIndexConfiguration, enableLocking, batchTables, tableFactory);
//                        jobStarted = true;
//                    } catch (Throwable t) {
//                        jobStarted = false;
//                        log.error("Error trying to start index build job for index " + indexName, t);
//                    }
//
//                    if (jobStarted) {
//                        ActiveBatchBuildInfo jobInfo = new ActiveBatchBuildInfo();
//                        jobInfo.setSubmitTime(System.currentTimeMillis());
//                        jobInfo.setJobId(job.getJobID().toString());
//                        jobInfo.setTrackingUrl(job.getTrackingURL());
//                        jobInfo.setBatchIndexConfiguration(batchIndexConfiguration);
//                        index.setActiveBatchBuildInfo(jobInfo);
//
//                        index.setBatchBuildState(IndexBatchBuildState.BUILDING);
//
//                        indexerModel.updateIndexInternal(index);
//
//                        log.info(
//                                "Started index build job for index " + indexName + ", job ID =  " + jobInfo.getJobId());
//                    } else {
//                        // The job failed to start. To test this case, configure an incorrect jobtracker address.
//                        BatchBuildInfo jobInfo = new BatchBuildInfo();
//                        jobInfo.setJobId("failed-" + System.currentTimeMillis());
//                        jobInfo.setSubmitTime(System.currentTimeMillis());
//                        jobInfo.setJobState("failed to start, check logs on " + hostName);
//                        jobInfo.setBatchIndexConfiguration(batchIndexConfiguration);
//                        jobInfo.setSuccess(false);
//
//                        index.setLastBatchBuildInfo(jobInfo);
//                        index.setBatchBuildState(IndexBatchBuildState.INACTIVE);
//
//                        indexerModel.updateIndexInternal(index);
//                    }
//
//                }
//            } finally {
//                indexerModel.unlockIndex(lock);
//            }
//        } catch (Throwable t) {
//            log.error("Error trying to start index build job for index " + indexName, t);
//        }
    }

    private byte[] getBatchIndexConfiguration(IndexerDefinition indexer) {
        if (indexer.getBatchIndexConfiguration() != null) {
            return indexer.getBatchIndexConfiguration();
        } else if (indexer.getDefaultBatchIndexConfiguration() != null) {
            return indexer.getDefaultBatchIndexConfiguration();
        } else {
            return this.fullTableScanConf;
        }
    }

    private void prepareDeleteIndex(String indexerName) {
        // We do not have to take a lock on the indexer, since once in delete state the indexer cannot
        // be modified anymore by ordinary users.
        boolean canBeDeleted = false;
        try {
            // Read current situation of record and assure it is still actual
            IndexerDefinition indexer = indexerModel.getFreshIndexer(indexerName);
            if (indexer.getLifecycleState() == IndexerDefinition.LifecycleState.DELETE_REQUESTED) {
                canBeDeleted = true;

                String queueSubscriptionId = indexer.getSubscriptionId();
                if (queueSubscriptionId != null) {
                    sepModel.removeSubscription(indexer.getSubscriptionId());
                    // We leave the subscription ID in the indexer definition FYI
                }

                if (indexer.getActiveBatchBuildInfo() != null) {
                    JobClient jobClient = getJobClient();
                    String jobId = indexer.getActiveBatchBuildInfo().getJobId();
                    RunningJob job = jobClient.getJob(jobId);
                    if (job != null) {
                        job.killJob();
                        log.info("Kill indexer build job for indexer " + indexerName + ", job ID =  " + jobId);
                    }
                    // Just to be sure...
                    jobStatusWatcher.assureWatching(indexer.getName(), indexer.getActiveBatchBuildInfo().getJobId());
                    canBeDeleted = false;
                }

                if (!canBeDeleted) {
                    indexer = new IndexerDefinitionBuilder()
                            .startFrom(indexer).lifecycleState(IndexerDefinition.LifecycleState.DELETING).build();
                    indexerModel.updateIndexerInternal(indexer);
                }
            } else if (indexer.getLifecycleState() == IndexerDefinition.LifecycleState.DELETING) {
                // Check if the build job is already finished, if so, allow delete
                if (indexer.getActiveBatchBuildInfo() == null) {
                    canBeDeleted = true;
                }
            }
        } catch (Throwable t) {
            log.error("Error preparing deletion of indexer " + indexerName, t);
        }

        if (canBeDeleted) {
            deleteIndex(indexerName);
        }
    }

    private void deleteIndex(String indexerName) {
        // delete model
        boolean failedToDeleteIndexer = false;
        try {
            indexerModel.deleteIndexerInternal(indexerName);
        } catch (Throwable t) {
            log.error("Failed to delete indexer " + indexerName, t);
            failedToDeleteIndexer = true;
        }

        if (failedToDeleteIndexer) {
            try {
                IndexerDefinition indexer = indexerModel.getFreshIndexer(indexerName);
                indexer = new IndexerDefinitionBuilder().startFrom(indexer)
                        .lifecycleState(IndexerDefinition.LifecycleState.DELETE_FAILED).build();
                indexerModel.updateIndexerInternal(indexer);
            } catch (Throwable t) {
                log.error("Failed to set indexer state to " + IndexerDefinition.LifecycleState.DELETE_FAILED, t);
            }
        }
    }

    private class MyListener implements IndexerModelListener {
        @Override
        public void process(IndexerModelEvent event) {
            try {
                // Let the events be processed by another thread. Especially important since
                // we take ZkLock's in the event handlers (see ZkLock javadoc).
                eventWorker.putEvent(event);
            } catch (InterruptedException e) {
                log.info("IndexerMaster.IndexerModelListener interrupted.");
            }
        }
    }

    private class EventWorker implements Runnable {

        private BlockingQueue<IndexerModelEvent> eventQueue = new LinkedBlockingQueue<IndexerModelEvent>();

        private boolean stop;

        private Thread thread;

        public synchronized void shutdown(boolean interrupt) throws InterruptedException {
            stop = true;
            eventQueue.clear();

            if (!thread.isAlive()) {
                return;
            }

            if (interrupt)
                thread.interrupt();
            thread.join();
            thread = null;
        }

        public synchronized void start() throws InterruptedException {
            if (thread != null) {
                log.warn("EventWorker start was requested, but old thread was still there. Stopping it now.");
                thread.interrupt();
                thread.join();
            }
            eventQueue.clear();
            stop = false;
            thread = new Thread(this, "IndexerMasterEventWorker");
            thread.start();
        }

        public void putEvent(IndexerModelEvent event) throws InterruptedException {
            if (stop) {
                throw new RuntimeException("This EventWorker is stopped, no events should be added.");
            }
            eventQueue.put(event);
        }

        @Override
        public void run() {
            long startedAt = System.currentTimeMillis();

            while (!stop && !Thread.interrupted()) {
                try {
                    IndexerModelEvent event = null;
                    while (!stop && event == null) {
                        event = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
                    }

                    if (stop || event == null || Thread.interrupted()) {
                        return;
                    }

                    // Warn if the queue is getting large, but do not do this just after we started, because
                    // on initial startup a fake update event is added for every defined index, which would lead
                    // to this message always being printed on startup when more than 10 indexes are defined.
                    int queueSize = eventQueue.size();
                    if (queueSize >= 10 && (System.currentTimeMillis() - startedAt > 5000)) {
                        log.warn("EventWorker queue getting large, size = " + queueSize);
                    }

                    if (event.getType() == INDEXER_ADDED || event.getType() == INDEXER_UPDATED) {
                        IndexerDefinition indexer = null;
                        try {
                            indexer = indexerModel.getIndexer(event.getIndexerName());
                        } catch (IndexerNotFoundException e) {
                            // ignore, indexer has meanwhile been deleted, we will get another event for this
                        }

                        if (indexer != null) {
                            if (indexer.getLifecycleState() == IndexerDefinition.LifecycleState.DELETE_REQUESTED ||
                                    indexer.getLifecycleState() == IndexerDefinition.LifecycleState.DELETING) {
                                prepareDeleteIndex(indexer.getName());

                                // in case of delete, we do not need to handle any other cases
                                continue;
                            }

                            if (needsSubscriptionIdAssigned(indexer)) {
                                assignSubscription(indexer.getName());
                            }

                            if (needsSubscriptionIdUnassigned(indexer)) {
                                unassignSubscription(indexer.getName());
                            }

                            if (needsBatchBuildStart(indexer)) {
                                startFullIndexBuild(indexer.getName());
                            }

                            if (indexer.getActiveBatchBuildInfo() != null) {
                                jobStatusWatcher
                                        .assureWatching(indexer.getName(), indexer.getActiveBatchBuildInfo().getJobId());
                            }
                        }
                    }

                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    log.error("Error processing indexer model event in IndexerMaster.", t);
                }
            }
        }
    }

    private class JobStatusWatcher implements Runnable {
        /**
         * Key = index name, value = job ID.
         */
        private Map<String, String> runningJobs = new ConcurrentHashMap<String, String>(10, 0.75f, 2);

        private boolean stop; // do not rely only on Thread.interrupt since some libraries eat interruptions

        private Thread thread;

        public JobStatusWatcher() {
        }

        public synchronized void shutdown(boolean interrupt) throws InterruptedException {
            stop = true;
            runningJobs.clear();

            if (!thread.isAlive()) {
                return;
            }

            if (interrupt)
                thread.interrupt();
            thread.join();
            thread = null;
        }

        public synchronized void start() throws InterruptedException {
            if (thread != null) {
                log.warn("JobStatusWatcher start was requested, but old thread was still there. Stopping it now.");
                thread.interrupt();
                thread.join();
            }
            runningJobs.clear();
            stop = false;
            thread = new Thread(this, "IndexerBatchJobWatcher");
            thread.start();
        }

        @Override
        public void run() {
            JobClient jobClient = null;
            while (!stop && !Thread.interrupted()) {
                try {
                    Thread.sleep(2000);

                    for (Map.Entry<String, String> jobEntry : runningJobs.entrySet()) {
                        if (stop || Thread.interrupted()) {
                            return;
                        }

                        if (jobClient == null) {
                            // We only create the JobClient the first time we need it, to avoid that the
                            // repository fails to start up when there is no JobTracker running.
                            jobClient = getJobClient();
                        }

                        RunningJob job = jobClient.getJob(jobEntry.getValue());

                        if (job == null) {
                            markJobComplete(jobEntry.getKey(), jobEntry.getValue(), false, "job unknown", null);
                        } else if (job.isComplete()) {
                            String jobState = jobStateToString(job.getJobState());
                            boolean success = job.isSuccessful();
                            markJobComplete(jobEntry.getKey(), jobEntry.getValue(), success, jobState,
                                    job.getCounters());
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    log.error("Error in index job status watcher thread.", t);
                }
            }
        }

        public synchronized void assureWatching(String indexerName, String jobName) {
            if (stop) {
                throw new RuntimeException(
                        "Job Status Watcher is stopped, should not be asked to monitor jobs anymore.");
            }
            runningJobs.put(indexerName, jobName);
        }

        private void markJobComplete(String indexerName, String jobId, boolean success, String jobState,
                Counters counters) {
            try {
                // Lock internal bypasses the index-in-delete-state check, which does not matter (and might cause
                // failure) in our case.
                String lock = indexerModel.lockIndexerInternal(indexerName, false);
                try {
                    // Read current situation of record and assure it is still actual
                    IndexerDefinition indexer = indexerModel.getFreshIndexer(indexerName);

                    ActiveBatchBuildInfo activeJobInfo = indexer.getActiveBatchBuildInfo();

                    if (activeJobInfo == null) {
                        // This might happen if we got some older update event on the indexer right after we
                        // marked this job as finished.
                        log.error("Unexpected situation: indexer build job completed but indexer does not have an active" +
                                " build job. Index: " + indexer.getName() + ", job: " + jobId + ". Ignoring this event.");
                        runningJobs.remove(indexerName);
                        return;
                    } else if (!activeJobInfo.getJobId().equals(jobId)) {
                        // I don't think this should ever occur: a new job will never start before we marked
                        // this one as finished, especially since we lock when creating/updating indexes.
                        log.error("Abnormal situation: indexer is associated with index build job " +
                                activeJobInfo.getJobId() + " but expected job " + jobId + ". Will mark job as" +
                                " done anyway.");
                    }

                    BatchBuildInfoBuilder jobInfoBuilder = new BatchBuildInfoBuilder();
                    jobInfoBuilder.jobState(jobState);
                    jobInfoBuilder.success(success);
                    jobInfoBuilder.jobId(jobId);
                    jobInfoBuilder.batchIndexConfiguration(activeJobInfo.getBatchIndexConfiguration());

                    if (activeJobInfo != null) {
                        jobInfoBuilder.submitTime(activeJobInfo.getSubmitTime());
                        jobInfoBuilder.trackingUrl(activeJobInfo.getTrackingUrl());
                    }

                    if (counters != null) {
                        jobInfoBuilder.counter(getCounterKey(Task.Counter.MAP_INPUT_RECORDS),
                                counters.getCounter(Task.Counter.MAP_INPUT_RECORDS));
                        jobInfoBuilder.counter(getCounterKey(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS),
                                counters.getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS));
                        jobInfoBuilder.counter(getCounterKey(JobInProgress.Counter.NUM_FAILED_MAPS),
                                counters.getCounter(JobInProgress.Counter.NUM_FAILED_MAPS));
                        // TODO
//                        jobInfo.addCounter(getCounterKey(IndexBatchBuildCounters.NUM_FAILED_RECORDS),
//                                counters.getCounter(IndexBatchBuildCounters.NUM_FAILED_RECORDS));
                    }

                    indexer = new IndexerDefinitionBuilder()
                        .lastBatchBuildInfo(jobInfoBuilder.build())
                        .activeBatchBuildInfo(null)
                        .batchIndexingState(BatchIndexingState.INACTIVE)
                        .build();

                    runningJobs.remove(indexerName);
                    indexerModel.updateIndexerInternal(indexer);

                    log.info("Marked indexer build job as finished for indexer " + indexerName + ", job ID =  " + jobId);

                } finally {
                    indexerModel.unlockIndexer(lock, true);
                }
            } catch (Throwable t) {
                log.error("Error trying to mark index build job as finished for indexer " + indexerName, t);
            }
        }

        private String getCounterKey(Enum key) {
            return key.getClass().getName() + ":" + key.name();
        }

        private String jobStateToString(int jobState) {
            String result = "unknown";
            switch (jobState) {
                case JobStatus.FAILED:
                    result = "failed";
                    break;
                case JobStatus.KILLED:
                    result = "killed";
                    break;
                case JobStatus.PREP:
                    result = "prep";
                    break;
                case JobStatus.RUNNING:
                    result = "running";
                    break;
                case JobStatus.SUCCEEDED:
                    result = "succeeded";
                    break;
            }
            return result;
        }
    }
}
