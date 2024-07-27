package io.dazzleduck.combiner.util;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ArrowIpcCsvPrinter {
    public static void main(String[] args) throws IOException {
        InputStream stream;
        if(args.length > 0){
            stream = new FileInputStream(args[0]);
        } else {
            stream = System.in;
        }

        try(var allocator = new RootAllocator();
            var reader = new ArrowStreamReader(stream, allocator)) {
            while (reader.loadNextBatch()){
                var vsr = reader.getVectorSchemaRoot();
                System.out.println(vsr.contentToTSVString());
            }
        }
    }
}
