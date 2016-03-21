package shortPath;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.Charsets;
import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class queryInputPE  extends ProcessingElement{

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

