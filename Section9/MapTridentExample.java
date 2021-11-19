package com.packt.tridentExamples;

import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class MapTridentExample implements MapFunction{

	@Override
	public Values execute(TridentTuple arg0) {
		// TODO Auto-generated method stub
		return new Values(arg0.getString(0).toLowerCase());
	}

}
