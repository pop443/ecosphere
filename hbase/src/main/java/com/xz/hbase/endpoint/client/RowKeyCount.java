package com.xz.hbase.endpoint.client;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;
import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.endpoint.protocol.Statistics.CountRequest;
import com.xz.hbase.endpoint.protocol.Statistics.CountResponse;
import com.xz.hbase.endpoint.protocol.Statistics.RowKeyCountService;
import com.xz.hbase.util.TableOperation;

public class RowKeyCount extends TableOperation{
	
	private BigDecimal count = new BigDecimal(0) ;
	
	private String startKey = null ;
	private String endKey = null ;
	
	public RowKeyCount(String startKey,String endKey){
		this.startKey = startKey ;
		this.endKey = endKey ;
	}
	
	public BigDecimal getCount() {
		return count;
	}
	@Override
	protected void doSomeThings() throws IOException {
		final CountRequest countRequest = CountRequest.newBuilder().setStartKey(startKey).setEndKey(endKey).build() ;
		Call<RowKeyCountService, Long> call = new Call<RowKeyCountService, Long>() {
			
			@Override
			public Long call(RowKeyCountService service) throws IOException {
				ServerRpcController controller = new ServerRpcController() ;
				BlockingRpcCallback<CountResponse> rpcCallback = new BlockingRpcCallback<CountResponse>() ;
				service.getRowCount(controller, countRequest, rpcCallback);
				CountResponse countResponse = rpcCallback.get() ;
				if (controller.failedOnException()) {
					throw controller.getFailedOn() ;
				}
				long count = countResponse.getCount() ;
				return count;
			}
		};
		try {
			Map<byte[], Long> map = hTable.coprocessorService(RowKeyCountService.class,Bytes.toBytes(startKey),Bytes.toBytes(endKey),call) ;
			Iterator<Long> it = map.values().iterator() ;
			while (it.hasNext()) {
				long simpleCount = it.next() ;
				count = count.add(new BigDecimal(simpleCount)) ;
				System.out.println(simpleCount);
			}
		} catch (ServiceException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		String startKey = "" ;
		String endKey = "" ;
		RowKeyCount rowKeyCount = new RowKeyCount(startKey, endKey) ;
		rowKeyCount.handle(TableName.valueOf(HbaseConf.TABLENAME)) ;
		System.out.println(rowKeyCount.getCount().toString());		
	}

	
}
