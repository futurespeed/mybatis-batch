package org.fs.common;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.fs.util.ClassUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import lombok.Getter;

public class BatchDAO {
    private SqlSessionFactory sqlSessionFactory;
    private JdbcTemplate jdbcTemplate;

    public <T, R, E> int[][] batchUpdate(ClassUtils.SerialFunction<T, R> function, List<E> dataList, int batchSize) {
        ClassUtils.FunctionInfo functionInfo = ClassUtils.getFunctionInfo(function);
        return batchUpdate(functionInfo.getClassName(), functionInfo.getMethodName(), dataList, batchSize);
    }

    public <T, R, E> int[][] batchUpdate(Class<T> cls, ClassUtils.SerialFunction<T, R> function, List<E> dataList,
            int batchSize) {
        ClassUtils.FunctionInfo functionInfo = ClassUtils.getFunctionInfo(cls, function);
        return batchUpdate(functionInfo.getClassName(), functionInfo.getMethodName(), dataList, batchSize);
    }

    public <E> int[][] batchUpdate(String namespace, String updateId, List<E> dataList, int batchSize) {
        BatisParamSetter<E> batisParamSetter = new BatisParamSetter<>(sqlSessionFactory, namespace, updateId,
                dataList.get(0));
        int[][] result = jdbcTemplate.batchUpdate(batisParamSetter.getMappedSql(), dataList, batchSize,
                batisParamSetter);

        // 清理改命名空间下的缓存
        Configuration configuration = sqlSessionFactory.getConfiguration();
        if (configuration.getCacheNames().contains(namespace)) {
            MappedStatement mappedStatement = configuration.getMappedStatement(namespace + "." + updateId);
            if (mappedStatement.isFlushCacheRequired()) {
                Cache cache = configuration.getCache(namespace);
                cache.clear();
            }
        }
        return result;
    }

    public static class BatisParamSetter<E> implements ParameterizedPreparedStatementSetter<E> {
        private SqlSessionFactory sqlSessionFactory;
        @Getter
        private String mappedSql;
        private List<ParameterMapping> mappings;

        public BatisParamSetter(SqlSessionFactory sqlSessionFactory, String namespace, String updateId,
                Object parameterObject) {
            this.sqlSessionFactory = sqlSessionFactory;
            BoundSql boundSql = this.sqlSessionFactory.getConfiguration()
                    .getMappedStatement(namespace + "." + updateId)
                    .getSqlSource().getBoundSql(parameterObject);
            this.mappedSql = boundSql.getSql();
            this.mappings = boundSql.getParameterMappings();
        }

        @Override
        public void setValues(PreparedStatement ps, E argument) throws SQLException {
            try {
                int i = 0;
                for (ParameterMapping mapping : this.mappings) {
                    i++;
                    Object value = null;
                    if (argument instanceof java.util.Map) {
                        value = ((java.util.Map) argument).get(mapping.getProperty());
                    } else {
                        Field field = argument.getClass().getDeclaredField(mapping.getProperty());
                        field.setAccessible(true);
                        value = field.get(argument);
                    }
                    JdbcType jdbcType = mapping.getJdbcType();
                    if (null == value && null == jdbcType) {
                        jdbcType = this.sqlSessionFactory.getConfiguration().getJdbcTypeForNull();
                    }
                    TypeHandler typeHandler = mapping.getTypeHandler();
                    typeHandler.setParameter(ps, i, value, jdbcType);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("set statement value error", e);
            }
        }

    }
}
