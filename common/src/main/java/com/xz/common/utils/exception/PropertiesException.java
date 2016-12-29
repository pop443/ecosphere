package com.xz.common.utils.exception;

import java.io.IOException;

/**
 * falcon -- 2016/11/22.
 */
public class PropertiesException extends IOException{
    private static final long serialVersionUID = 1L;

    public PropertiesException(String msg){
        super(msg) ;
    }

    public PropertiesException(Exception e){
        super("parse Properties Exception:"+e.getMessage()) ;
    }

}
