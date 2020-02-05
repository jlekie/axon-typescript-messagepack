import { AProtocol, AProtocolReader, AProtocolWriter, ITransport, IProtocol, IProtocolReader, IProtocolWriter, ITransportMetadata, VolatileTransportMetadata, TransportMessage } from '@jlekie/axon';
import { ReadableStreamBuffer, WritableStreamBuffer } from 'stream-buffers';
import { DecodeStream, EncodeStream } from 'msgpack-lite';
import * as MessagePack from 'msgpack-lite';
// import * as MsgPack from 'msgpack';

export interface IMessagePackProtocol extends IProtocol {
    readonly decoderChunkSize: number;
}

export class MessagePackProtocol extends AProtocol implements IMessagePackProtocol {
    public readonly decoderChunkSize: number;

    public constructor(decoderChunkSize: number) {
        super();

        this.decoderChunkSize = decoderChunkSize;
    }

    public async writeData(transport: ITransport, metadata: ITransportMetadata, handler: (protocolWriter: IProtocolWriter) => void): Promise<void> {
        const buffer = new WritableStreamBuffer();
        const encoderStream = MessagePack.createEncodeStream();

        encoderStream.pipe(buffer);

        const writer = new MessagePackProtocolWriter(encoderStream);
        handler(writer);

        encoderStream.end();

        const data = buffer.getContents();
        if (!data)
            throw new Error('Buffer empty');

        await transport.send(new TransportMessage(data, VolatileTransportMetadata.fromMetadata(metadata)));
    }
    public async writeTaggedData(transport: ITransport, messageId: string, metadata: ITransportMetadata, handler: (protocolWriter: IProtocolWriter) => void): Promise<void> {
        const buffer = new WritableStreamBuffer();
        const encoderStream = MessagePack.createEncodeStream();

        encoderStream.pipe(buffer);

        const writer = new MessagePackProtocolWriter(encoderStream);
        handler(writer);

        encoderStream.end();

        const data = buffer.getContents();
        if (!data)
            throw new Error('Buffer empty');

        await transport.sendTagged(messageId, new TransportMessage(data, VolatileTransportMetadata.fromMetadata(metadata)));
    }

    public async readData<TResult = void>(transport: ITransport, handler: (protocolReader: IProtocolReader, metadata: ITransportMetadata) => TResult): Promise<TResult> {
        const transportMessage = await transport.receive();

        const buffer = new ReadableStreamBuffer({ chunkSize: transportMessage.payload.length || this.decoderChunkSize });
        const decoderStream = MessagePack.createDecodeStream();

        const result = await new Promise<TResult>((resolve, reject) => {
            buffer.once('readable', async () => {
                try {
                    buffer.read();

                    const reader = new MessagePackProtocolReader(decoderStream);
                    resolve(handler(reader, transportMessage.metadata));
                }
                catch (err) {
                    reject(err);
                }
            });

            buffer.put(transportMessage.payload);
            buffer.stop();

            decoderStream.pause();
            buffer.pipe(decoderStream);
        });

        return result;
    }
    public async readTaggedData<TResult = void>(transport: ITransport, messageId: string, handler: (protocolReader: IProtocolReader, metadata: ITransportMetadata) => TResult): Promise<TResult> {
        const transportMessage = await transport.receiveTagged(messageId);

        const buffer = new ReadableStreamBuffer({ chunkSize: transportMessage.payload.length || this.decoderChunkSize });
        const decoderStream = MessagePack.createDecodeStream();

        const result = await new Promise<TResult>((resolve, reject) => {
            buffer.once('readable', async () => {
                try {
                    buffer.read();

                    const reader = new MessagePackProtocolReader(decoderStream);
                    resolve(handler(reader, transportMessage.metadata));
                }
                catch (err) {
                    reject(err);
                }
            });

            buffer.put(transportMessage.payload);
            buffer.stop();

            decoderStream.pause();
            buffer.pipe(decoderStream);
        });

        return result;
    }

    public async readBufferedTaggedData<TResult = void>(transport: ITransport, handler: (protocolReader: IProtocolReader, messageId: string, metadata: ITransportMetadata) => TResult): Promise<TResult> {
        const taggedTransportMessage = await transport.receiveBufferedTagged();

        const buffer = new ReadableStreamBuffer({ chunkSize: taggedTransportMessage.message.payload.length || this.decoderChunkSize });
        const decoderStream = MessagePack.createDecodeStream();

        const result = await new Promise<TResult>((resolve, reject) => {
            buffer.once('readable', async () => {
                try {
                    buffer.read();

                    const reader = new MessagePackProtocolReader(decoderStream);
                    resolve(handler(reader, taggedTransportMessage.id, taggedTransportMessage.message.metadata));
                }
                catch (err) {
                    reject(err);
                }
            });

            buffer.put(taggedTransportMessage.message.payload);
            buffer.stop();

            decoderStream.pause();
            buffer.pipe(decoderStream);
        });

        return result;
    }

    public async writeAndReadData<TResult = void>(transport: ITransport, metadata: ITransportMetadata, handler: (protocolWriter: IProtocolWriter) => void): Promise<(readHandler: ((protocolReader: IProtocolReader, metadata: ITransportMetadata) => TResult)) => Promise<TResult>> {
        const buffer = new WritableStreamBuffer();
        const encoderStream = MessagePack.createEncodeStream();

        encoderStream.pipe(buffer);

        const writer = new MessagePackProtocolWriter(encoderStream);
        handler(writer);

        encoderStream.end();

        const data = buffer.getContents();
        if (!data)
            throw new Error('Buffer empty');

        const receiveHandler = await transport.sendAndReceive(new TransportMessage(data, VolatileTransportMetadata.fromMetadata(metadata)));

        return async (readHandler) => {
            const { payload: data, metadata } = await receiveHandler();

            const buffer = new ReadableStreamBuffer({ chunkSize: data.length || this.decoderChunkSize });
            const decoderStream = MessagePack.createDecodeStream();

            const result = await new Promise<TResult>((resolve, reject) => {
                buffer.once('readable', async () => {
                    try {
                        buffer.read();

                        const reader = new MessagePackProtocolReader(decoderStream);
                        resolve(readHandler(reader, metadata));
                    }
                    catch (err) {
                        reject(err);
                    }
                });

                buffer.put(data);
                buffer.stop();

                decoderStream.pause();
                buffer.pipe(decoderStream);
            });

            return result;
        };
    }
}

export interface IMessagePackProtocolReader extends IProtocolReader {
    readonly decoderStream: DecodeStream;
}
export class MessagePackProtocolReader extends AProtocolReader implements IMessagePackProtocolReader {
    public readonly decoderStream: DecodeStream;

    public constructor(decoderStream: DecodeStream) {
        super();

        this.decoderStream = decoderStream;
    }

    public readStringValue(): string {
        return this.read<string>();
    }
    public readBooleanValue(): boolean {
        return this.read<boolean>();
    }
    public readByteValue(): number {
        return this.read<number>();
    }
    public readShortValue(): number {
        return this.read<number>();
    }
    public readIntegerValue(): number {
        return this.read<number>();
    }
    public readLongValue(): BigInt {
        return this.read<BigInt>();
    }
    public readFloatValue(): number {
        return this.read<number>();
    }
    public readDoubleValue(): number {
        return this.read<number>();
    }
    public readEnumValue<T>(): T {
        return this.read<T>();
    }

    private read<T>(): T {
        const data = this.decoderStream.read();
        if (data === null)
            throw new Error('Buffer Underrun');

        return data;
    }
}

export interface IMessagePackProtocolWriter extends IProtocolWriter {
    readonly encoderStream: EncodeStream;
}

export class MessagePackProtocolWriter extends AProtocolWriter implements IMessagePackProtocolWriter {
    public readonly encoderStream: EncodeStream;

    public constructor(encoderStream: EncodeStream) {
        super();

        this.encoderStream = encoderStream;
    }

    public writeStringValue(value: string) {
        this.write(value);
    }
    public writeBooleanValue(value: boolean) {
        this.write(value);
    }
    public writeByteValue(value: number) {
        this.write(value);
    }
    public writeShortValue(value: number) {
        this.write(value);
    }
    public writeIntegerValue(value: number) {
        this.write(value);
    }
    public writeLongValue(value: BigInt) {
        this.write(value);
    }
    public writeFloatValue(value: number) {
        this.write(value);
    }
    public writeDoubleValue(value: number) {
        this.write(value);
    }
    public writeEnumValue<T>(value: T) {
        this.write(value);
    }

    private write(data: any) {
        this.encoderStream.write(data);
    }
}