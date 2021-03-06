package org.corfudb.util.serializer;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 2/18/16.
 */
@Slf4j
public class PrimitiveSerializer implements ISerializer {

    public static final Map<Byte, DeserializerFunction> DeserializerMap =
            Arrays.stream(Primitives.values())
                    .collect(Collectors.toMap(Primitives::getTypeNum, Primitives::getDeserializer));
    public static final Map<Class, SerializerFunction> SerializerMap = getSerializerMap();

    @SuppressWarnings("unchecked")
    private static Map<Class, SerializerFunction> getSerializerMap() {
        ImmutableMap.Builder b = ImmutableMap.<Class, SerializerFunction>builder();
        Arrays.stream(Primitives.values())
                .forEach(e -> {
                            b.put(e.getClassType(), e.getSerializer());
                            if (e.getPrimitiveClass() != null) {
                                b.put(e.getPrimitiveClass(), e.getSerializer());
                            }
                        }
                );
        return b.build();
    }

    @SuppressWarnings("unchecked")
    static <T, R> void writeArray(T[] o, ByteBuf b, BiFunction<ByteBuf, R, ByteBuf> applyFunc) {
        int length = Array.getLength(o);
        b.writeInt(length);
        Arrays.stream(o)
                .forEach(i -> applyFunc.apply(b, (R) i));
    }

    @SuppressWarnings("unchecked")
    static <T, R> T[] readArray(ByteBuf b, Function<ByteBuf, T> applyFunc, Function<Integer, T[]> arrayGen) {
        int length = b.readInt();
        T[] r = arrayGen.apply(length);
        for (int i = 0; i < length; i++) {
            r[i] = applyFunc.apply(b);
        }
        return r;
    }

    static void writeBytes(Object o, ByteBuf b) {
        int length = Array.getLength(o);
        b.writeInt(length);
        b.writeBytes((byte[]) o);
    }

    static byte[] readBytes(ByteBuf b, CorfuRuntime rt) {
        int length = b.readInt();
        byte[] bytes = new byte[length];
        b.readBytes(bytes, 0, length);
        return bytes;
    }

    /**
     * Deserialize an object from a given byte buffer.
     *
     * @param b The bytebuf to deserialize.
     * @return The deserialized object.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Object deserialize(ByteBuf b, CorfuRuntime rt) {
        byte type = b.readByte();
        DeserializerFunction d = DeserializerMap.get(type);
        return d.deserialize(b, rt);
    }

    /**
     * Serialize an object into a given byte buffer.
     *
     * @param o The object to serialize.
     * @param b The bytebuf to serialize it into.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void serialize(Object o, ByteBuf b) {
        if (o.getClass().getName().contains("$ByteBuddy$")) {
            ((SerializerFunction<Object>) Primitives.CORFU_SMR.getSerializer()).serialize(o, b);
        } else {
            SerializerFunction f = SerializerMap.get(o.getClass());
            if (f == null) {
                throw new RuntimeException("Unsupported class for serialization: " + o.getClass());
            }
            f.serialize(o, b);
        }
    }

    enum Primitives {
        BYTE(0, Byte.class, byte.class, (o, b) -> b.writeByte(o), (b, r) -> b.readByte()),
        SHORT(1, Short.class, short.class, (o, b) -> b.writeShort(o), (b, r) -> b.readShort()),
        INTEGER(2, Integer.class, int.class, (o, b) -> b.writeInt(o), (b, r) -> b.readInt()),
        LONG(3, Long.class, long.class, (o, b) -> b.writeLong(o), (b, r) -> b.readLong()),
        BOOLEAN(4, Boolean.class, boolean.class, (o, b) -> b.writeBoolean(o), (b, r) -> b.readBoolean()),
        DOUBLE(5, Double.class, double.class, (o, b) -> b.writeDouble(o), (b, r) -> b.readDouble()),
        FLOAT(6, Float.class, float.class, (o, b) -> b.writeFloat(o), (b, r) -> b.readFloat()),
        BYTE_ARRAY(7, byte[].class, Byte[].class, PrimitiveSerializer::writeBytes, PrimitiveSerializer::readBytes),
        SHORT_ARRAY(8, Short[].class, short[].class, (o, b) -> writeArray(o, b, ByteBuf::writeShort),
                (b, r) -> readArray(b, ByteBuf::readShort, Short[]::new)),
        INTEGER_ARRAY(9, Integer[].class, int[].class, (o, b) -> writeArray(o, b, ByteBuf::writeInt),
                (b, r) -> readArray(b, ByteBuf::readInt, Integer[]::new)),
        LONG_ARRAY(10, Long[].class, long[].class, (o, b) -> writeArray(o, b, ByteBuf::writeLong),
                (b, r) -> readArray(b, ByteBuf::readLong, Long[]::new)),
        BOOLEAN_ARRAY(11, Boolean[].class, boolean[].class, (o, b) -> writeArray(o, b, ByteBuf::writeBoolean),
                (b, r) -> readArray(b, ByteBuf::readBoolean, Boolean[]::new)),
        DOUBLE_ARRAY(12, Double[].class, double[].class, (o, b) -> writeArray(o, b, ByteBuf::writeDouble),
                (b, r) -> readArray(b, ByteBuf::readDouble, Double[]::new)),
        FLOAT_ARRAY(13, Float[].class, float[].class, (o, b) -> writeArray(o, b, ByteBuf::writeFloat),
                (b, r) -> readArray(b, ByteBuf::readFloat, Float[]::new)),
        STRING(14, String.class, null, (o, b) -> {
            b.writeInt(o.length());
            b.writeBytes(o.getBytes());
        },
                (b, r) -> {
                    int length = b.readInt();
                    byte[] bs = new byte[length];
                    b.readBytes(bs, 0, length);
                    return new String(bs);
                }),
        CORFU_SMR(15, Object.class, null, (o, b) -> {
            String className = o.getClass().toString();
            String SMRClass = className.split("\\$")[0];
            className = "CorfuObject";
            byte[] classNameBytes = className.getBytes();
            b.writeShort(classNameBytes.length);
            b.writeBytes(classNameBytes);
            byte[] SMRClassNameBytes = SMRClass.getBytes();
            b.writeShort(SMRClassNameBytes.length);
            b.writeBytes(SMRClassNameBytes);
            try {
                Field f = o.getClass().getDeclaredField("_corfuStreamID");
                f.setAccessible(true);
                UUID id = (UUID) f.get(o);
                log.trace("Serializing a CorfuObject of type {} as a stream pointer to {}", SMRClass, id);
                b.writeLong(id.getMostSignificantBits());
                b.writeLong(id.getLeastSignificantBits());
            } catch (NoSuchFieldException | IllegalAccessException nsfe) {
                log.error("Error serializing fields");
                throw new RuntimeException(nsfe);
            }
        },
                (b, r) -> {
                    int SMRClassNameLength = b.readShort();
                    byte[] SMRClassNameBytes = new byte[SMRClassNameLength];
                    b.readBytes(SMRClassNameBytes, 0, SMRClassNameLength);
                    String SMRClassName = new String(SMRClassNameBytes);
                    try {
                        return r.getObjectsView().build()
                                .setStreamID(new UUID(b.readLong(), b.readLong()))
                                .setType(Class.forName(SMRClassName))
                                .open();
                    } catch (ClassNotFoundException cnfe) {
                        log.error("Exception during deserialization!", cnfe);
                        throw new RuntimeException(cnfe);
                    }
                });

        @Getter
        public final byte typeNum;
        @Getter
        public final Class<?> classType;
        @Getter
        public final Class<?> primitiveClass;
        @Getter
        public final SerializerFunction<?> serializer;
        @Getter
        public final DeserializerFunction deserializer;
        <T> Primitives(int typeNum, Class<T> classType, Class<?> primitiveClass,
                       SerializerFunction<T> serializer, DeserializerFunction<T> deserializer) {
            this.typeNum = (byte) typeNum;
            this.classType = classType;
            this.primitiveClass = primitiveClass;
            this.serializer = serializer;
            this.deserializer = deserializer;
        }
    }


    @FunctionalInterface
    interface DeserializerFunction<T> {
        T deserialize(ByteBuf b, CorfuRuntime r);
    }

    @FunctionalInterface
    interface SerializerFunction<T> {
        void serialize(T o, ByteBuf b);
    }
}
