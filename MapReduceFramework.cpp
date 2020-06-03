#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <iostream>
#include <vector>
#include <pthread.h>
#include <atomic>
#include <set>

void* mapThreadFunc(void*);
void* shuffleThreadFunc(void*);

class JobContext{
public:
    typedef struct{
        int threadID;
        JobContext* job;
        std::vector<std::pair<K2*, V2*>> mapOutput;
        pthread_mutex_t mapOutputMutex;
    } ThreadContext;

    JobState state;
    int multiThreadLevel;
    std::vector<ThreadContext> threadContext;
    const MapReduceClient& client;
    const InputVec& inputVec;
    OutputVec& outputVec;
    std::map<K2*, std::vector<V2 *>, K2PointerComp> intermediateMap;
    pthread_t* threads;
    int numK1Elements;
    std::atomic<int> numK2Elements{0};
    std::atomic<int> mapCounter{0};
    int shuffleCopiedElements = 0;

    std::set<K2*, K2PointerComp> uniqueKeys;
    std::vector<K2*> uniqueKeysHelper;
    std::atomic<int> reduceCounter{0};
    pthread_mutex_t reduceLock;
    pthread_cond_t shuffleDone;
    pthread_mutex_t stateLock;

    JobContext(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec,
        int multiThreadLevel):
            state({UNDEFINED_STAGE, 0.0}),
            multiThreadLevel(multiThreadLevel),
            client(client),
            inputVec(inputVec),
            outputVec(outputVec),
            threads(new pthread_t[multiThreadLevel]),
            numK1Elements(inputVec.size()),
            numK2Elements(0),
            reduceLock(),
            shuffleDone(),
            stateLock()
    {
        if (pthread_cond_init(&shuffleDone, nullptr) != 0){
            std::cerr << "system error: failed to init cond variable" << std::endl;
            exit(1);
        }

        if (pthread_mutex_init(&reduceLock, nullptr) != 0){
            std::cerr << "system error: failed to init cond variable" << std::endl;
            exit(1);
        }

        for (int i = 0; i < multiThreadLevel; i++) {
            threadContext.push_back(ThreadContext{i, this});
        }

        for (int i = 0; i < multiThreadLevel - 1; i++){
            if (pthread_create(&threads[i], nullptr, mapThreadFunc, (void*) (&threadContext.at(i))) != 0) {
                std::cerr << "System error: thread creation failed" << std::endl;
            }
        }

        if (pthread_create(&threads[multiThreadLevel - 1], nullptr, shuffleThreadFunc,
                           (void *)this) != 0) {
            std::cerr << "system error: thread creation failed" << std::endl;
            exit(1);
        }
    }

    void wait(){
        for (int i = 0; i < multiThreadLevel; ++i) {
            if (pthread_join(threads[i], nullptr) != 0){
                std::cerr << "system error: thread join failed" << std::endl;
                exit(1);
            }
        }
    }

    ~JobContext(){
        delete []threads;
    }
};


void reduceUtil(JobContext::ThreadContext *tc){
    int old_value = (tc->job->reduceCounter)++;
    if (old_value == 0) {
        tc->job->state = {REDUCE_STAGE, 0};
    }
    while ((unsigned)old_value < tc->job->uniqueKeysHelper.size()){
        if (pthread_mutex_lock(&tc->job->stateLock) != 0){
            std::cerr << "system error: error on pthread_mutex_lock" << std::endl;
            exit(1);
        }

        float progress = 100 * ((float)(old_value + 1) / tc->job->uniqueKeysHelper.size());
        if (progress > tc->job->state.percentage){
            tc->job->state.percentage = progress;
        }

        if (pthread_mutex_unlock(&tc->job->stateLock) != 0){
            std::cerr << "system error: error on pthread_mutex_unlock" << std::endl;
            exit(1);
        }

        K2* k = tc->job->uniqueKeysHelper.at(old_value);
        tc->job->client.reduce(k, tc->job->intermediateMap[k], tc);
        old_value = (tc->job->reduceCounter)++;
    }
}

void* mapThreadFunc(void *threadContext){
    auto tc = (JobContext::ThreadContext*)threadContext;
    int old_value = (tc->job->mapCounter)++;
    if (old_value == 0){
        tc->job->state = {MAP_STAGE, 0};
    }
    while (old_value < tc->job->numK1Elements){
        float progress = 100 * ((float)(old_value + 1) / tc->job->numK1Elements);

        if (pthread_mutex_lock(&tc->job->stateLock) != 0){
            std::cerr << "system error: error on pthread_mutex_lock" << std::endl;
            exit(1);
        }

        if (progress > tc->job->state.percentage){
            tc->job->state.percentage = progress;
        }

        if (pthread_mutex_unlock(&tc->job->stateLock) != 0){
            std::cerr << "system error: error on pthread_mutex_unlock" << std::endl;
            exit(1);
        }

        tc->job->client.map(tc->job->inputVec.at(old_value).first, tc->job->inputVec.at(old_value).second, tc);
        old_value = (tc->job->mapCounter)++;
    }

    if (old_value == tc->job->numK1Elements) {
        tc->job->state = {SHUFFLE_STAGE, 100 * ((float)tc->job->shuffleCopiedElements / tc->job->numK2Elements)};
    }

    if (pthread_mutex_lock(&tc->job->reduceLock) != 0){
        std::cerr << "system error: error on pthread_mutex_lock" << std::endl;
        exit(1);
    }

    while (tc->job->state.percentage < 100.0){
        pthread_cond_wait(&tc->job->shuffleDone, &tc->job->reduceLock);
    }

    if (pthread_mutex_unlock(&tc->job->reduceLock) != 0){
        std::cerr << "system error: error on pthread_mutex_unlock" << std::endl;
        exit(1);
    }
    reduceUtil(tc);
    return nullptr;
}


void* shuffleThreadFunc(void *jobContext){
    auto jc = (JobContext*)jobContext;

    std::vector<int> offsets(jc->multiThreadLevel - 1, 0);
    bool shuffleCompleted = false;
    while (!shuffleCompleted){
        for (JobContext::ThreadContext& tc: jc->threadContext){
            if (tc.threadID == jc->multiThreadLevel - 1){
                continue;
            }
            if (pthread_mutex_lock(&tc.mapOutputMutex) != 0){
                std::cerr << "system error: error on pthread_mutex_lock" << std::endl;
                exit(1);
            }

            int mapSize = tc.mapOutput.size();

            if (pthread_mutex_unlock(&tc.mapOutputMutex) != 0){
                std::cerr << "system error: error on pthread_mutex_unlock" << std::endl;
                exit(1);
            }

            while (offsets[tc.threadID] < mapSize) {
                if (pthread_mutex_lock(&tc.mapOutputMutex) != 0){
                    std::cerr << "system error: error on pthread_mutex_lock" << std::endl;
                    exit(1);
                }

                std::pair<K2*, V2*>* el = &tc.mapOutput.at(offsets.at(tc.threadID));

                if (pthread_mutex_unlock(&tc.mapOutputMutex) != 0){
                    std::cerr << "system error: error on pthread_mutex_unlock" << std::endl;
                    exit(1);
                }

                auto res = jc->uniqueKeys.insert(el->first);
                if (!res.second){
                    jc->intermediateMap.at(*res.first).push_back(el->second);

                } else {
                    jc->intermediateMap[el->first] = std::vector<V2*>();
                    jc->intermediateMap[el->first].push_back(el->second);
                }

                jc->shuffleCopiedElements++;

                if (jc->state.stage == SHUFFLE_STAGE){
                    jc->state.percentage = 100 * ((float)jc->shuffleCopiedElements / jc->numK2Elements);
                    if (jc->state.percentage == 100) {
                        shuffleCompleted = true;
                    }
                }
                offsets[tc.threadID]++;
            }
        }
    }

    if (pthread_mutex_lock(&jc->reduceLock) != 0){
        std::cerr << "system error: error on pthread_mutex_lock" << std::endl;
        exit(1);
    }

    for (auto &k: jc->uniqueKeys){
        jc->uniqueKeysHelper.push_back(k);
    }

    pthread_cond_broadcast(&jc->shuffleDone);

    if (pthread_mutex_unlock(&jc->reduceLock) != 0){
        std::cerr << "system error: error on pthread_mutex_unlock" << std::endl;
        exit(1);
    }

    reduceUtil(&jc->threadContext[jc->multiThreadLevel - 1]);
    return nullptr;
}


void emit2 (K2* key, V2* value, void* context) {
    auto tc = (JobContext::ThreadContext*)context;
    if (pthread_mutex_lock(&tc->mapOutputMutex) != 0){
        std::cerr << "system error: error on pthread_mutex_lock" << std::endl;
        exit(1);
    }

    tc->mapOutput.push_back({key, value});
    (tc->job->numK2Elements)++;

    if (pthread_mutex_unlock(&tc->mapOutputMutex) != 0){
        std::cerr << "system error: error on pthread_mutex_unlock" << std::endl;
        exit(1);
    }
}

void emit3 (K3* key, V3* value, void* context) {
    auto tc = (JobContext::ThreadContext*)context;
    tc->job->outputVec.push_back({key, value});
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    auto job = new JobContext(client, inputVec, outputVec, multiThreadLevel);
    return (JobHandle)job;

}

void waitForJob(JobHandle job) {
    auto jc = (JobContext*)job;
    jc->wait();
}


void getJobState(JobHandle job, JobState* state) {
    auto jc = (JobContext*)job;
    *state = jc->state;
}

void closeJobHandle(JobHandle job) {
    auto jc = (JobContext*)job;
    delete jc;
    job = nullptr;
    (void)job;
}
