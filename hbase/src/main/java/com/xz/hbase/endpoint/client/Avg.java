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
import com.xz.hbase.endpoint.protocol.Statistics.AvgRequest;
import com.xz.hbase.endpoint.protocol.Statistics.AvgResponse;
import com.xz.hbase.endpoint.protocol.Statistics.AvgService;
import com.xz.hbase.util.TableOperation;

public class Avg  extends TableOperation {

	private Double avg = null ;
	private String startKey = null ;
	private String endKey = null ;
	public Avg(String startKey,String endKey){
		this.startKey = startKey ;
		this.endKey = endKey ;
	}
	
	public Double getAvg() {
		return avg;
	}

	@Override
	protected void doSomeThings() throws IOException {
		final AvgRequest avgRequest = AvgRequest.newBuilder().setStartRowKey(startKey).setEndRowKey(endKey).build() ;
		Call<AvgService, String[]> call = new Call<AvgService, String[]>() {
			
			@Override
			public String[] call(AvgService service) throws IOException {
				ServerRpcController controller = new ServerRpcController() ;
				BlockingRpcCallback<AvgResponse> rpcCallBack = new BlockingRpcCallback<AvgResponse>() ;
				service.getResponse(controller, avgRequest, rpcCallBack);
				AvgResponse avgResponse = rpcCallBack.get() ;
				if (controller.failedOnException()) {
					throw controller.getFailedOn() ;
				}
				String num = avgResponse.getNum() ;
				String sum = avgResponse.getSum() ;
				String[] strings = new String[2] ;
				strings[0] = num ;
				strings[1] = sum ;
				return strings;
			}
		};
		
		try {
			Map<byte[], String[]> map = hTable.coprocessorService(AvgService.class, startKey==null?null:Bytes.toBytes(startKey),endKey==null?null:Bytes.toBytes(endKey) , call) ;
			Iterator<byte[]> it = map.keySet().iterator() ;
			BigDecimal numAll = new BigDecimal(0) ;
			BigDecimal sumAll = new BigDecimal(0) ;
			while (it.hasNext()) {
				byte[] region = it.next() ;
				String regionName = Bytes.toString(region) ;
				String[] strings = map.get(region) ;
				String num = strings[0] ;
				String sum = strings[1] ;
				numAll = numAll.add(new BigDecimal(num)) ;
				sumAll = sumAll.add(new BigDecimal(sum)) ;
				System.out.println("region块："+regionName+"数量："+num+"，总和："+sum);
			}
			System.out.println("总数量:"+numAll.toString()+",值总大小:"+sumAll.toString());
			//精度问题
			avg = sumAll.divide(numAll,6).doubleValue() ;
		} catch (ServiceException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		String startKey = "" ;
		String endKey = "" ;
		Avg avg = new Avg(startKey, endKey) ;
		avg.handle(TableName.valueOf(HbaseConf.TABLENAME)) ;
		System.out.println(avg.getAvg());
	}
	
}
