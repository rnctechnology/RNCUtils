package com.rnctech.common;

import java.sql.Types;

/**
 * Created by Zilin on 12/17/2020.
 */

public class NRDataType {

	public enum SourceTableType {
		TABLE, VIEW
	}

	public enum KeyType {
		PRIMARY, FOREIGN, CANDIDATE
	}

	public enum RnctechDataType {
		STRING(Types.VARCHAR), TEXT(Types.CLOB), CHAR(Types.CHAR), DATETIME(Types.TIMESTAMP), DATE(Types.DATE), TIME(
				Types.TIME), SHORT(Types.SMALLINT), INTEGER(Types.INTEGER), LONG(Types.BIGINT), BIGINT(
						Types.BIGINT), FLOAT(
								Types.FLOAT), DOUBLE(Types.DOUBLE), DECIMAL(Types.DECIMAL), BOOLEAN(Types.BIT);

		private int jdbcType;

		RnctechDataType(int jdbcType) {
			this.jdbcType = jdbcType;
		}

		public int jdbcType() {
			return jdbcType;
		}
	}

}
