package com.link.up.server.application;

import com.link.up.server.application.port.JobRepository;
import com.link.up.server.application.port.JobRuntimeScheduler;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Architecture guards for application-layer role ownership. */
public class JobApplicationRoleBoundaryTest {

    @Test
    public void applicationServiceShouldDelegateActiveStateAndLifecycle() {
        Set<Class<?>> fieldTypes =
                instanceFieldTypes(
                        JobApplicationService.class);

        assertTrue(
                fieldTypes.contains(
                        ActiveJobRegistry.class));
        assertTrue(
                fieldTypes.contains(
                        JobRuntimeLifecycle.class));
        assertFalse(
                fieldTypes.contains(
                        ConcurrentMap.class));
    }

    @Test
    public void runtimeLifecycleShouldDependOnPortsNotInfrastructure() {
        Set<Class<?>> fieldTypes =
                instanceFieldTypes(
                        JobRuntimeLifecycle.class);

        assertTrue(
                fieldTypes.contains(
                        JobRuntimeScheduler.class));
        assertTrue(
                fieldTypes.contains(
                        JobRepository.class));
        assertTrue(
                fieldTypes.contains(
                        ActiveJobRegistry.class));

        assertNoInfrastructureField(
                JobRuntimeLifecycle.class);
        assertNoInfrastructureField(
                JobRecoveryService.class);
    }

    private static Set<Class<?>> instanceFieldTypes(
            Class<?> type) {

        Set<Class<?>> types =
                new HashSet<Class<?>>();

        for (Field field : type.getDeclaredFields()) {
            if (!java.lang.reflect.Modifier.isStatic(
                    field.getModifiers())) {
                types.add(field.getType());
            }
        }

        return types;
    }

    private static void assertNoInfrastructureField(
            Class<?> type) {

        for (Field field : type.getDeclaredFields()) {
            String fieldType =
                    field.getGenericType()
                            .getTypeName();

            assertFalse(
                    type.getSimpleName()
                            + "."
                            + field.getName()
                            + " must not depend on concrete infrastructure",
                    fieldType.contains(
                            "com.link.up.server.infrastructure."));
        }
    }
}
