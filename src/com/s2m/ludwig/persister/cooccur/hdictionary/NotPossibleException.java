package com.s2m.ludwig.persister.cooccur.hdictionary;


/*************************************************************************
 * 
 *  Description
 *  -------
 *  
 *
 *  Remarks
 *  -------
 *  
 *
 *************************************************************************/
public class NotPossibleException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public NotPossibleException(String error, Throwable e) {
		super(error, e);
	}
}
