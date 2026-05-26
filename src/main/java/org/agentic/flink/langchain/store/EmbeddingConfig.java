package org.agentic.flink.langchain.store;

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
@TypeInfo(EmbeddingConfig.TypeInfo.class)
public class EmbeddingConfig implements Serializable {

  private AiModel embedModel;
  private EmbeddingStore embedStore;
  private Map<String, String> modelProperties;
  private Map<String, String> storeProperties;

  public static class TypeInfo extends TypeInfoFactory<EmbeddingConfig> {

    @Override
    public TypeInformation<EmbeddingConfig> createTypeInfo(
        Type type, Map<String, TypeInformation<?>> map) {
      return Types.POJO(
          EmbeddingConfig.class,
          Map.of(
              "embedModel",
              Types.ENUM(AiModel.class),
              "embedStore",
              Types.ENUM(EmbeddingStore.class),
              "modelProperties",
              Types.MAP(Types.STRING, Types.STRING),
              "storeProperties",
              Types.MAP(Types.STRING, Types.STRING)));
    }
  }
}
