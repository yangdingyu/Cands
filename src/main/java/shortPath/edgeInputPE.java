package shortPath;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.Charsets;
import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class edgeInputPE extends ProcessingElement{

    Streamable<Event> downStream;
    static Logger logger = LoggerFactory.getLogger(edgeInputPE.class);
    boolean firstEvent = true;
    long timeInterval = 0;

    /**
     * This method is called upon a new Event on an incoming stream
     */
    public void onEvent(Event event) {
    	downStream.put(event);

    }
    
        
    public void setDownStream(Streamable<Event> stream){
    	this.downStream = stream;
    	
    }
    
    public void setInterval(long interval){
    	this.timeInterval = interval;
    }
    
    @Override
    protected void onCreate() {
    }

    @Override
    protected void onRemove() {
    }
}
