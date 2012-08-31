/*
 * 
 * 
 */
package datatransfer.extract;

import datatransfer.Output;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author zgw@dongying.pbc
 */
public class DataExtractTask1 implements Runnable {

    public static final String STATE_FINISHED = "完成";
    public static final String STATE_RUNNING = "正在运行";
    public static final String STATE_READY = "就绪";
    public static final String STATE_WRONG = "出错停止";
    public static final String STATE_STOPPED = "用户终止";
    private static final String STATE_STOPPING = "正在终止";
    private int total;
    private int count = 0;
    private int perCount = 100;
    private String name;
    private String state;
    private List<TaskListener> taskListenerList = new ArrayList<TaskListener>();
    private Output output;

    public DataExtractTask1(String name, int total) {
        this.name = name;
        this.total = total;
        this.state = STATE_READY;
    }

    /**
     * 重新初始化
     */
    public void reInit() {
        if (!getState().equals(STATE_RUNNING)) {
            output.prompt("重新初始化" + getName());
            output.prompt("清空已导入数据：" + getName());
            setCount(0);
            this.setState(STATE_READY);
        } else {
            throw new IllegalStateException(getName() + "正在运行，不能重新初始化");
        }
    }

    public void run() {        
        if (getState().equals(STATE_READY)) {
            this.setState(STATE_RUNNING);
        }
        while (getState().equals(STATE_RUNNING)) {            
            try {
                Thread.sleep(500);
                if ((count + perCount) < total) {
                    setCount(count + perCount);
                } else {
                    setCount(total);
                    if (getState().equals(STATE_RUNNING)) {
                        setState(STATE_FINISHED);
                    }
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                this.setState(STATE_WRONG);
            }
        }
        if (getState().equals(STATE_STOPPING)) {
            setState(STATE_STOPPED);
        }
        getOutput().prompt(getName() + ":" + getState());
    }

    /**
     * @return the count
     */
    public int getCount() {
        return count;
    }

    /**
     * @param count the count to set
     */
    public void setCount(int count) {
        this.count = count;
        fireChange();
    }

    /**
     * @return the perCount
     */
    public int getPerCount() {
        return perCount;
    }

    /**
     * @param perCount the perCount to set
     */
    public void setPerCount(int perCount) {
        this.perCount = perCount;
        fireChange();
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
        fireChange();
    }

    /**
     * @return the state
     */
    public String getState() {
        return state;
    }

    /**
     * @param state the state to set
     */
    public void setState(String state) {
        String oldState = this.state;
        this.state = state;
        getOutput().info(getName() + "状态改变：from " + oldState + " to " + this.state);
        fireChange();
    }

    /**
     * @return the total
     */
    public int getTotal() {
        return total;
    }

    /**
     * @param total the total to set
     */
    public void setTotal(int total) {
        this.total = total;
        fireChange();
    }


    public Output getOutput() {
        return output;
    }


    public void setOutput(Output output) {
        this.output = output;
    }

    /**
     * 剩余行数
     * @return 
     */
    public int getRemain() {
        return total - count;
    }

    /**
     * 完成百分比
     * @return 
     */
    public String getPercent() {
        if (getTotal() != 0 && getCount() < getTotal()) {
            double percent = getCount() * 100.0 / getTotal();
            return new java.text.DecimalFormat("#.00").format(percent) + "%";
        } else {
            return "100%";
        }
    }

    @Override
    public String toString() {
        return "数据抽取线程，表：" + name;
    }

    public void stop() {
        if (!getState().equals(STATE_FINISHED)) {
            setState(STATE_STOPPING);
        }
    }

    private void fireChange() {
        for (TaskListener tl : taskListenerList) {
            tl.change();
        }
    }

    public void registerTaskListener(TaskListener taskListener) {
        taskListenerList.add(taskListener);
    }

    public void removeTaskListener(TaskListener taskListener) {
        taskListenerList.remove(taskListener);
    }
}
