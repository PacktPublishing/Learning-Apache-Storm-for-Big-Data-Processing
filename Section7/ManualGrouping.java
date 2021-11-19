package com.packt.stormstreamgrouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ManualGrouping implements CustomStreamGrouping,Serializable {

	//CustomStreamGrouping allows you to choose the task/component to which
	//you want to send the tuple

    private  List<Integer> targetTasks;

    //We need the targetTask id based on how much bolt we
    //instantiate
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks){

        this.targetTasks = targetTasks;
    };

    //Grouping Logic
    //Takes the tuple from a task
    //Informs which TaskID the tuple has to be sent to
    public List<Integer> chooseTasks(int taskId, List<Object> values){

        List<Integer> boltIds = new ArrayList<Integer>();
        
        //which bucket to choose to store the tuple
        Integer boltNum =  Integer.parseInt(values.get(1).toString()) % targetTasks.size();

        boltIds.add(targetTasks.get(boltNum));

        return boltIds;

    };

}
