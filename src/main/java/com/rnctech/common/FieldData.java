package com.rnctech.common;

/**
 * Column style definition
 * Created by Zilin on 12/17/2020.
 */

public class FieldData {
	public String name;
	public int type;
	public boolean nullable = true;
	public int precision = 0;
	public int scale = 0;

	public FieldData(String name, int type) {
		this.name = name;
		this.type = type;
	}

	public FieldData(String name, int type, boolean nullable, int precision, int scale) {
		this.name = name;
		this.type = type;
		this.nullable = nullable;
		this.precision = precision;
		this.scale = scale;
	}
}
