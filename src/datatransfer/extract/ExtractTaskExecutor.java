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

public class ExtractTaskExecutor {

    private ThreadPoolExecutor tpe;
    private List<DataExtractTask> taskList;

    public ExtractTaskExecutor(int poolSize, List<DataExtractTask> taskList) {
        this.taskList = taskList;
        BlockingQueue<Runnable> bq = new LinkedBlockingQueue<Runnable>();
        this.tpe = new ThreadPoolExecutor(poolSize, Integer.MAX_VALUE, 600, TimeUnit.SECONDS, bq);
    }

    /** 执行所有任务*/
    public void execute() {
        for (DataExtractTask task : taskList) {
            this.tpe.execute(task);
        }
    }

    /** 关闭线程池 */
    public void shutdown() {
        for (DataExtractTask task : taskList) {
            task.stop();
        }
        this.tpe.shutdown();
    }
}
