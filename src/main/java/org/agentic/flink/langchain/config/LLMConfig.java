package org.agentic.flink.langchain.config;

import org.agentic.flink.langchain.model.AiModel;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TypeInfo(LLMConfig.TypeInfo.class)
public class LLMConfig implements Serializable {

  private Long userId;
  private AiModel aiModel;
  private Map<String, String> properties;
  private String systemMessage;

  public static class TypeInfo extends TypeInfoFactory<LLMConfig> {
    @Override
    public TypeInformation<LLMConfig> createTypeInfo(
        Type type, Map<String, TypeInformation<?>> map) {
      return Types.POJO(
          LLMConfig.class,
          Map.of(
              "userId",
              Types.LONG,
              "aiModel",
              Types.ENUM(AiModel.class),
              "properties",
              Types.MAP(Types.STRING, Types.STRING),
              "systemMessage",
              Types.STRING));
    }
  }
}
