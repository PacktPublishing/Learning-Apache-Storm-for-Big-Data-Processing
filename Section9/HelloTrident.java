package com.packt.tridentExamples;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class HelloTrident extends BaseFunction{

	@Override
	public void execute(TridentTuple arg0, TridentCollector arg1) {
		// TODO Auto-generated method stub
		
        arg1.emit(new Values("Hello "+arg0.getString(0)+". Welcome to Trident"));
		
	}

}
