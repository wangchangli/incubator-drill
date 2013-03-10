/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.common.expression.fn;

import com.fasterxml.jackson.databind.ser.std.StaticListSerializerBase;
import org.apache.drill.common.expression.ArgumentValidators.AllowedTypeList;
import org.apache.drill.common.expression.ArgumentValidators.ComparableArguments;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.expression.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class BooleanFunctions implements CallProvider {
  static final Logger logger = LoggerFactory.getLogger(BooleanFunctions.class);

  @Override
  public FunctionDefinition[] getFunctionDefintions() {
    return new FunctionDefinition[] {
        FunctionDefinition.operator("or", new AllowedTypeList(2, Integer.MAX_VALUE, DataType.BOOLEAN), OutputTypeDeterminer.FIXED_BOOLEAN, "or", "||"),
        FunctionDefinition.operator("and", new AllowedTypeList(2, Integer.MAX_VALUE, DataType.BOOLEAN), OutputTypeDeterminer.FIXED_BOOLEAN, "and", "&&"),
        FunctionDefinition.operator("greater than", new ComparableArguments(2), OutputTypeDeterminer.FIXED_BOOLEAN, ">"),
        FunctionDefinition.operator("less than", new ComparableArguments(2), OutputTypeDeterminer.FIXED_BOOLEAN, "<"),
        FunctionDefinition.operator("equal", new ComparableArguments(2), OutputTypeDeterminer.FIXED_BOOLEAN, "==", "<>"),
        FunctionDefinition.operator("greater than or equal to", new ComparableArguments(2), OutputTypeDeterminer.FIXED_BOOLEAN, ">="),
        FunctionDefinition.operator("less than or equal to", new ComparableArguments(2), OutputTypeDeterminer.FIXED_BOOLEAN, "<="), };

  }

 public static FunctionDefinition getFunctionDefintion(String name){

     Map<String, Integer> funtionMap = new HashMap<String,Integer>();
     funtionMap.put("or",0);
     funtionMap.put("and",1);
     funtionMap.put("greater than",2);
     funtionMap.put("less than",3);
     funtionMap.put("equal",4);
     funtionMap.put("greater than or equal to",5);
     funtionMap.put("less than or equal to",6);

     return new BooleanFunctions().getFunctionDefintions()[funtionMap.get(name)];
 }


}
