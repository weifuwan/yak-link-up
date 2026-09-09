package com.link.up.connector.file.s3;

import com.link.up.connector.file.internal.FileEntry;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3FileStorageTest {

    private S3Client client;

    @Before
    public void setUp() {
        client = mock(S3Client.class);
    }

    @Test
    public void shouldPreferExactObjectOverPrefixList() {
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().contentLength(5L).build());

        S3FileStorage storage = new S3FileStorage(client, "bucket");
        List<FileEntry> files = storage.listFiles("prefix/rows.csv", true);

        assertEquals(1, files.size());
        assertEquals("prefix/rows.csv", files.get(0).getFileKey());
        Mockito.verify(client, Mockito.never()).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    public void shouldFallBackToPrefixListWhenExactObjectMissing() {
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("missing").build());
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder()
                        .isTruncated(false)
                        .contents(s3Object("prefix/rows.csv", 5))
                        .build());

        S3FileStorage storage = new S3FileStorage(client, "bucket");
        List<FileEntry> files = storage.listFiles("prefix/rows.csv", true);

        assertEquals(1, files.size());
        Mockito.verify(client).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    public void shouldPageThroughListObjectsV2() {
        ListObjectsV2Response firstPage = ListObjectsV2Response.builder()
                .isTruncated(true)
                .nextContinuationToken("token-1")
                .contents(s3Object("a.csv", 10), s3Object("m.csv", 20))
                .build();
        ListObjectsV2Response lastPage = ListObjectsV2Response.builder()
                .isTruncated(false)
                .contents(s3Object("z.csv", 30))
                .build();
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(firstPage, lastPage);

        S3FileStorage storage = new S3FileStorage(client, "bucket");
        List<FileEntry> files = storage.listFiles("prefix/", true);

        assertEquals(3, files.size());
        assertEquals("a.csv", files.get(0).getFileKey());
        assertEquals(30L, files.get(2).getSize());
    }

    @Test
    public void shouldRequestByteRangeOnOpen() throws Exception {
        byte[] payload = "0123456789ABCDEF".getBytes(StandardCharsets.UTF_8);
        GetObjectRequest[] captured = new GetObjectRequest[1];
        when(client.getObject(any(GetObjectRequest.class))).thenAnswer(invocation -> {
            captured[0] = invocation.getArgument(0);
            return responseStream(payload);
        });

        S3FileStorage storage = new S3FileStorage(client, "bucket");
        InputStream input = storage.openRange("prefix/rows.csv", 4L, 6L);

        byte[] read = new byte[16];
        int total = 0;
        int chunk;
        while ((chunk = input.read(read)) >= 0) {
            total += chunk;
        }
        assertEquals(6, total);
        assertEquals("bytes=4-9", captured[0].range());
    }

    @Test
    public void shouldReturnFalseOnMissingKey() {
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("missing").build());

        S3FileStorage storage = new S3FileStorage(client, "bucket");

        assertFalse(storage.exists("prefix/missing.csv"));
    }

    @Test
    public void shouldReturnTrueOnExistingKey() {
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(null);

        S3FileStorage storage = new S3FileStorage(client, "bucket");

        assertTrue(storage.exists("prefix/rows.csv"));
        Mockito.verify(client).headObject(any(HeadObjectRequest.class));
    }

    private static software.amazon.awssdk.services.s3.model.S3Object s3Object(String key, long size) {
        return software.amazon.awssdk.services.s3.model.S3Object.builder()
                .key(key)
                .size(size)
                .build();
    }

    private static ResponseInputStream<GetObjectResponse> responseStream(byte[] bytes) {
        return new ResponseInputStream<GetObjectResponse>(
                GetObjectResponse.builder().build(),
                new ByteArrayInputStream(bytes) {
                    @Override
                    public void close() throws java.io.IOException {
                        // nothing extra to release in the fake
                    }
                });
    }
}
