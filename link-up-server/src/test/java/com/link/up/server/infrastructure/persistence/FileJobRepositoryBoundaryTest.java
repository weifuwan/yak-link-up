package com.link.up.server.infrastructure.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Architecture guards for durable Worker state persistence roles. */
public class FileJobRepositoryBoundaryTest {

    @Test
    public void repositoryShouldDelegateFileIoToStateFileStore() {
        Set<Class<?>> fieldTypes =
                instanceFieldTypes(
                        FileJobRepository.class);

        assertTrue(
                fieldTypes.contains(
                        JobStateFileStore.class));
        assertFalse(fieldTypes.contains(ObjectMapper.class));
        assertFalse(fieldTypes.contains(Path.class));
        assertFalse(fieldTypes.contains(FileOutputStream.class));
    }

    @Test
    public void fileStoreShouldOwnJsonAndPathIo() {
        Set<Class<?>> fieldTypes =
                instanceFieldTypes(
                        JobStateFileStore.class);

        assertTrue(fieldTypes.contains(ObjectMapper.class));
        assertTrue(fieldTypes.contains(Path.class));
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
}
