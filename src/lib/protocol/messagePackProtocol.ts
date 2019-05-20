import { AProtocol, AProtocolReader, AProtocolWriter, ITransport, IProtocol, IProtocolReader, IProtocolWriter } from '@jlekie/axon';
import { ReadableStreamBuffer, WritableStreamBuffer } from 'stream-buffers';
import { DecodeStream, EncodeStream } from 'msgpack-lite';
import * as MessagePack from 'msgpack-lite';

export interface IMessagePackProtocol extends IProtocol {
    readonly decoderChunkSize: number;
}

export class MessagePackProtocol extends AProtocol implements IMessagePackProtocol {
    public readonly decoderChunkSize: number;

    public constructor(decoderChunkSize: number) {
        super();

        this.decoderChunkSize = decoderChunkSize;
    }

    public async writeData(transport: ITransport, metadata: Map<string, Buffer>, handler: (protocolWriter: IProtocolWriter) => void): Promise<void> {
        const buffer = new WritableStreamBuffer();
        const encoderStream = MessagePack.createEncodeStream();

        encoderStream.pipe(buffer);

        const writer = new MessagePackProtocolWriter(transport, this, encoderStream);
        handler(writer);

        encoderStream.end();

        const data = buffer.getContents();
        if (!data)
            throw new Error('Buffer empty');

        await transport.send(data, metadata);
    }
    public async readData<TResult = void>(transport: ITransport, handler: (protocolReader: IProtocolReader, metadata: Map<string, Buffer>) => TResult): Promise<TResult> {
        const { data, metadata } = await transport.receive();

        const buffer = new ReadableStreamBuffer({ chunkSize: data.length || this.decoderChunkSize });
        const decoderStream = MessagePack.createDecodeStream();

        const result = await new Promise<TResult>((resolve, reject) => {
            buffer.once('readable', async () => {
                try {
                    const reader = new MessagePackProtocolReader(transport, this, decoderStream);
                    resolve(handler(reader, metadata));
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
    }
    public async writeAndReadData<TResult = void>(transport: ITransport, metadata: Map<string, Buffer>, handler: (protocolWriter: IProtocolWriter) => void): Promise<(readHandler: ((protocolReader: IProtocolReader, metadata: Map<string, Buffer>) => TResult)) => Promise<TResult>> {
        const buffer = new WritableStreamBuffer();
        const encoderStream = MessagePack.createEncodeStream();

        encoderStream.pipe(buffer);

        const writer = new MessagePackProtocolWriter(transport, this, encoderStream);
        handler(writer);

        encoderStream.end();

        const data = buffer.getContents();
        if (!data)
            throw new Error('Buffer empty');

        const receiveHandler = await transport.sendAndReceive(data, metadata);

        return async (readHandler) => {
            const { data, metadata } = await receiveHandler();

            const buffer = new ReadableStreamBuffer({ chunkSize: data.length || this.decoderChunkSize });
            const decoderStream = MessagePack.createDecodeStream();

            const result = await new Promise<TResult>((resolve, reject) => {
                buffer.once('readable', async () => {
                    try {
                        const reader = new MessagePackProtocolReader(transport, this, decoderStream);
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

    public constructor(transport: ITransport, protocol: IProtocol, decoderStream: DecodeStream) {
        super(transport, protocol);

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
    public readLongValue(): number {
        return this.read<number>();
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

    public constructor(transport: ITransport, protocol: IProtocol, encoderStream: EncodeStream) {
        super(transport, protocol);

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
    public writeLongValue(value: number) {
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