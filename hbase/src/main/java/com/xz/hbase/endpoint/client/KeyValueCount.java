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
import com.xz.hbase.endpoint.protocol.Statistics.KeyValueCountService;
import com.xz.hbase.util.TableOperation;

public class KeyValueCount extends TableOperation{
	
	private BigDecimal count = new BigDecimal(0) ;
	
	private String startKey = null ;
	private String endKey = null ;
	
	public KeyValueCount(String startKey,String endKey){
		this.startKey = startKey ;
		this.endKey = endKey ;
	}
	
	public BigDecimal getCount() {
		return count;
	}
	
	@Override
	protected void doSomeThings() throws IOException {
		final CountRequest countRequest = CountRequest.newBuilder().setStartKey(startKey).setEndKey(endKey).build() ;
		Call<KeyValueCountService, Long> call = new Call<KeyValueCountService, Long>() {
			
			@Override
			public Long call(KeyValueCountService service) throws IOException {
				ServerRpcController controller = new ServerRpcController() ;
				BlockingRpcCallback<CountResponse> rpcCallback = new BlockingRpcCallback<CountResponse>() ;
				service.getkeyvalueCount(controller, countRequest, rpcCallback);
				CountResponse countResponse = rpcCallback.get() ;
				if (controller.failedOnException()) {
					throw controller.getFailedOn() ;
				}
				long count = countResponse.getCount() ;
				return count;
			}
		};
		
		try {
			Map<byte[], Long> map = hTable.coprocessorService(KeyValueCountService.class,startKey==null?null:Bytes.toBytes(startKey),endKey==null?null:Bytes.toBytes(endKey) , call) ;
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
		String startKey = "14176_20051107023411" ;
		String endKey = "14176_20051107023413" ;
		KeyValueCount keyValueCount = new KeyValueCount(startKey, endKey) ;
		keyValueCount.handle(TableName.valueOf(HbaseConf.TABLENAME)) ;
		System.out.println(keyValueCount.getCount().toString());
	}
}
