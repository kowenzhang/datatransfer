/*
 * 
 * 
 */
package datatransfer.extract;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author zgw@dongying.pbc
 */
public class QueryTaskExecutor {

    private ThreadPoolExecutor tpe;
    private List<QueryExecuteTask> taskList;

    public QueryTaskExecutor(int poolSize, List<QueryExecuteTask> taskList) {
        this.taskList = taskList;
        BlockingQueue<Runnable> bq = new LinkedBlockingQueue<Runnable>();
        this.tpe = new ThreadPoolExecutor(poolSize, Integer.MAX_VALUE, 600, TimeUnit.SECONDS, bq);
    }

    /** 执行所有任务*/
    public void execute() {
        for (QueryExecuteTask task : taskList) {
            this.tpe.execute(task);
        }
    }

    /** 关闭线程池 */
    public void shutdown() {
        for (QueryExecuteTask task : taskList) {            
            task.stop();
        }
        this.tpe.shutdown();
    }
}
