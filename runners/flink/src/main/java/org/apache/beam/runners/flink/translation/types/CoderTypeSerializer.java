/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink.translation.types;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.DataInputViewWrapper;
import org.apache.beam.runners.flink.translation.wrappers.DataOutputViewWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.*;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Flink {@link org.apache.flink.api.common.typeutils.TypeSerializer} for Beam {@link
 * org.apache.beam.sdk.coders.Coder Coders}.
 */
@SuppressWarnings({
        "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
        "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CoderTypeSerializer<T> extends TypeSerializer<T> {

    private final Coder<T> coder;

    /**
     * {@link SerializablePipelineOptions} deserialization will cause {@link
     * org.apache.beam.sdk.io.FileSystems} registration needed for {@link
     * org.apache.beam.sdk.transforms.Reshuffle} translation.
     */
    private final SerializablePipelineOptions pipelineOptions;

    private final boolean fasterCopy;

    public CoderTypeSerializer(Coder<T> coder, SerializablePipelineOptions pipelineOptions) {
        Preconditions.checkNotNull(coder);
        Preconditions.checkNotNull(pipelineOptions);
        this.coder = coder;
        this.pipelineOptions = pipelineOptions;

        FlinkPipelineOptions options = pipelineOptions.get().as(FlinkPipelineOptions.class);
        this.fasterCopy = options.getFasterCopy();
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public CoderTypeSerializer<T> duplicate() {
        return new CoderTypeSerializer<>(coder, pipelineOptions);
    }

    @Override
    public T createInstance() {
        return null;
    }

    @Override
    public T copy(T t) {
        if (fasterCopy) {
            return t;
        }
        try {
            return CoderUtils.clone(coder, t);
        } catch (CoderException e) {
            throw new RuntimeException("Could not clone.", e);
        }
    }

    @Override
    public T copy(T t, T reuse) {
        return copy(t);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T t, DataOutputView dataOutputView) throws IOException {
        DataOutputViewWrapper outputWrapper = new DataOutputViewWrapper(dataOutputView);
        coder.encode(t, outputWrapper);
    }

    @Override
    public T deserialize(DataInputView dataInputView) throws IOException {
        try {
            DataInputViewWrapper inputWrapper = new DataInputViewWrapper(dataInputView);
            return coder.decode(inputWrapper);
        } catch (CoderException e) {
            Throwable cause = e.getCause();
            if (cause instanceof EOFException) {
                throw (EOFException) cause;
            } else {
                throw e;
            }
        }
    }

    @Override
    public T deserialize(T t, DataInputView dataInputView) throws IOException {
        return deserialize(dataInputView);
    }

    @Override
    public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
        serialize(deserialize(dataInputView), dataOutputView);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CoderTypeSerializer that = (CoderTypeSerializer) o;
        return coder.equals(that.coder);
    }

    @Override
    public int hashCode() {
        return coder.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new LegacySnapshot<>(this);
    }

    /**
     * A legacy snapshot which does not care about schema compatibility.
     */
    public static class LegacySnapshot<T> extends VersionedIOReadableWritable implements TypeSerializerSnapshot<T> {

        static final int ADAPTER_VERSION = 0x7a53c4f0;

        private ClassLoader userCodeClassLoader;

        private TypeSerializer<T> serializer;

        public LegacySnapshot(CoderTypeSerializer<T> serializer) {
            this.serializer = serializer;
        }

        public LegacySnapshot() {
        }

        @Internal
        public final void setPriorSerializer(TypeSerializer<T> serializer) {
            this.serializer = org.apache.flink.util.Preconditions.checkNotNull(serializer);
        }

        @Internal
        public final void setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
            this.userCodeClassLoader = org.apache.flink.util.Preconditions.checkNotNull(userCodeClassLoader);
        }

        @Override
        public int getCurrentVersion() {
            return 0;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            try (ByteArrayOutputStreamWithPos streamWithPos =
                         new ByteArrayOutputStreamWithPos()) {
                InstantiationUtil.serializeObject(streamWithPos, serializer);
                out.writeInt(streamWithPos.getPosition());
                out.write(streamWithPos.getBuf(), 0, streamWithPos.getPosition());
            }
            write(out);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader) throws IOException {
            int serializerBytes = in.readInt();
            byte[] buffer = new byte[serializerBytes];

            ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
            try (InstantiationUtil.FailureTolerantObjectInputStream ois =
                         new InstantiationUtil.FailureTolerantObjectInputStream(
                                 new ByteArrayInputStream(buffer), classLoader)) {

                Thread.currentThread().setContextClassLoader(classLoader);
                setPriorSerializer((TypeSerializer<T>) ois.readObject());
            } catch (Exception e) {
                throw new UnloadableTypeSerializerException(e, buffer);
            } finally {
                Thread.currentThread().setContextClassLoader(previousClassLoader);
            }
            setUserCodeClassLoader(classLoader);
            read(in);
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            return serializer;
        }

        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializer<T> newSerializer) {
            // We assume compatibility because we don't have a way of checking schema compatibility
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }

        @Override
        public int getVersion() {
            return ADAPTER_VERSION;
        }
    }

    @Override
    public String toString() {
        return "CoderTypeSerializer{" + "coder=" + coder + '}';
    }

    @Internal
    private static class UnloadableTypeSerializerException extends IOException {

        private static final long serialVersionUID = 1L;

        private final byte[] serializerBytes;

        /**
         * Creates a new exception, with the cause of the read error and the original serializer
         * bytes.
         *
         * @param cause           the cause of the read error.
         * @param serializerBytes the original serializer bytes.
         */
        public UnloadableTypeSerializerException(Exception cause, byte[] serializerBytes) {
            super(cause);
            this.serializerBytes = org.apache.flink.util.Preconditions.checkNotNull(serializerBytes);
        }

        public byte[] getSerializerBytes() {
            return serializerBytes;
        }
    }
}
