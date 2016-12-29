package com.xz.hbase.endpoint.statisticImpl;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.xz.hbase.endpoint.protocol.Statistics.AvgRequest;
import com.xz.hbase.endpoint.protocol.Statistics.AvgResponse;
import com.xz.hbase.endpoint.protocol.Statistics.AvgService;

public class AvgKeyValueImpl extends AvgService implements Coprocessor,CoprocessorService{
	private RegionCoprocessorEnvironment env ;
	
	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		}else {
			throw new CoprocessorException("must be load on a table region") ;
		}
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
	}

	@Override
	public void getResponse(RpcController controller, AvgRequest request, RpcCallback<AvgResponse> done) {
		Scan scan = new Scan() ;
		String startKey = request.getStartRowKey() ;
		String endKey = request.getEndRowKey() ;
		if (startKey!=null && !startKey.equals("")) {
			scan.setStartRow(Bytes.toBytes(startKey)) ;
		}
		if (endKey!=null && !endKey.equals("")) {
			scan.setStopRow(Bytes.toBytes(endKey)) ;
		}
		
		AvgResponse avgResponse = null ;
		RegionScanner regionScanner = null ;
		try {
			regionScanner = env.getRegion().getScanner(scan) ;
			List<Cell> results = new ArrayList<Cell>() ;
			boolean hasMore = false ;
			//数量
			long count = 0 ;
			//值累加
			BigDecimal bigDecimal = new BigDecimal(0) ;
			do {
				hasMore = regionScanner.next(results) ;
				for (int i = 0; i < results.size(); i++) {
					Cell cell = results.get(i) ;
					byte[] thisValue = CellUtil.cloneValue(cell) ;
					String value_str = Bytes.toString(thisValue) ;
					double value = Double.parseDouble(value_str) ;
					bigDecimal = bigDecimal.add(new BigDecimal(value)) ;
					count++ ;
				}
				results.clear();
			}while (hasMore);
			avgResponse = AvgResponse.newBuilder().setNum(count+"").setSum(bigDecimal.toString()).build() ;
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (regionScanner!=null) {
				try {
					regionScanner.close();
				} catch (IOException e) {
					
				}
			}
		}
		done.run(avgResponse);
	}

}
