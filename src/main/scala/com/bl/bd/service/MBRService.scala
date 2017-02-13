package com.bl.bd.service

import com.bl.bd.bean.Polygon
import com.infomatiq.jsi.Rectangle;

trait  MBRService extends Serializable{
	
	/**
	 * get MBR of polygon 
	 * @param polygon
	 * @return
	 */
	def  getMBROfPolygon(polygon: Polygon) : Rectangle;

}
