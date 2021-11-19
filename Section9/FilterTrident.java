package com.packt.tridentExamples;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class FilterTrident extends BaseFilter{

	@Override
	public boolean isKeep(TridentTuple arg0) {
		// TODO Auto-generated method stub
		
		return arg0.getString(0).length() > 5;
	}

}
