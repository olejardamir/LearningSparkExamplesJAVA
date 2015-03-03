package BasicAvgWithKryo;

import java.io.Serializable;

import org.apache.spark.serializer.KryoRegistrator;

import BasicAvg.AvgCount;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

public class AvgRegistrator implements KryoRegistrator, Serializable{

	public void registerClasses(Kryo kryo) {
		kryo.register(AvgCount.class, new FieldSerializer(kryo,AvgCount.class)); 
	}
	
	

}
