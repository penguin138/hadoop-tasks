import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by nastya on 07.01.16.
 */
class ComplexKey implements WritableComparable<ComplexKey> {

    private LongWritable rowBlockIndex;
    private LongWritable columnBlockIndex;
    private LongWritable numberOfCopy;

    public ComplexKey(){
        rowBlockIndex = new LongWritable();
        columnBlockIndex = new LongWritable();
        numberOfCopy = new LongWritable();
    }

    public ComplexKey(ComplexKey otherKey) {
        this(otherKey.rowBlockIndex,otherKey.columnBlockIndex,otherKey.numberOfCopy);
    }

    public long getRowBlockIndex() {
        return rowBlockIndex.get();
    }

    public long getColumnBlockIndex() {
        return columnBlockIndex.get();
    }

    public long getNumberOfCopy() {
        return numberOfCopy.get();
    }



    @Override
    public String toString() {
        return "("+rowBlockIndex.get()+", "+columnBlockIndex.get()+", "+numberOfCopy.get()+")";
    }

    public ComplexKey(LongWritable newRowBlockIndex, LongWritable newColumnBlockIndex, LongWritable newNumberOfCopy) {
        this();
        rowBlockIndex.set(newRowBlockIndex.get());
        columnBlockIndex.set(newColumnBlockIndex.get());
        numberOfCopy.set(newNumberOfCopy.get());
    }

    @Override
    public int compareTo(ComplexKey o) {
        int rowIndexResult = rowBlockIndex.compareTo(o.rowBlockIndex);
        int columnIndexResult = columnBlockIndex.compareTo(o.columnBlockIndex);
        int copyResult = numberOfCopy.compareTo(o.numberOfCopy);
        if (rowIndexResult == 0) {
            if (columnIndexResult == 0) {
                return copyResult;
            } else {
                return columnIndexResult;
            }
        } else {
            return rowIndexResult;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        rowBlockIndex.write(dataOutput);
        columnBlockIndex.write(dataOutput);
        numberOfCopy.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rowBlockIndex.readFields(dataInput);
        columnBlockIndex.readFields(dataInput);
        numberOfCopy.readFields(dataInput);
    }
}

class ComplexValue implements Writable {
    private LongWritable inBlockRowIndex;
    private LongWritable inBlockColumnIndex;
    private DoubleWritable value;
    private IntWritable matrixNumber;
    public ComplexValue() {
        inBlockColumnIndex = new LongWritable();
        inBlockRowIndex = new LongWritable();
        value = new DoubleWritable();
        matrixNumber = new IntWritable();
    }

    @Override
    public String toString() {
        return "(" + inBlockRowIndex.get() + ", " + inBlockColumnIndex.get() + ", " + value.get() + ", " + matrixNumber.get() + ")";
    }

    public int getMatrixNumber() {
        return matrixNumber.get();
    }
    public LongWritable getInBlockRowIndex() {
        return inBlockRowIndex;
    }

    public LongWritable getInBlockColumnIndex() {
        return inBlockColumnIndex;
    }
    public double getValue() {
        return value.get();
    }

    public ComplexValue(LongWritable newInBlockRowIndex, LongWritable newInBlockColumnIndex, DoubleWritable newValue, IntWritable newMatrixNumber) {
        this();
        inBlockRowIndex.set(newInBlockRowIndex.get());
        inBlockColumnIndex.set(newInBlockColumnIndex.get());
        value.set(newValue.get());
        matrixNumber.set(newMatrixNumber.get());
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        inBlockRowIndex.write(dataOutput);
        inBlockColumnIndex.write(dataOutput);
        value.write(dataOutput);
        matrixNumber.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        inBlockRowIndex.readFields(dataInput);
        inBlockColumnIndex.readFields(dataInput);
        value.readFields(dataInput);
        matrixNumber.readFields(dataInput);
    }
}



class MyMapper extends Mapper<Object,Text, ComplexKey, ComplexValue > {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        long I = Long.parseLong(conf.get("I"));
        long K = Long.parseLong(conf.get("K"));
        long J = Long.parseLong(conf.get("J"));
        long rowBlockSize = Long.parseLong(conf.get("BlockI"));
        long columnBlockSize  = Long.parseLong(conf.get("BlockJ"));
        int matrixIntNumber = getMatrixNumber(context);
        IntWritable matrixNumber = new IntWritable(matrixIntNumber);
        long numberOfBlockColumns;
        long numberOfBlockRows;
        long numberOfBlockColumnsAnotherMatrix;
        long numberOfBlockRowsAnotherMatrix;
        if (matrixIntNumber == 0) {
            numberOfBlockColumns = K / columnBlockSize;
            numberOfBlockRows = I / rowBlockSize;
            numberOfBlockColumnsAnotherMatrix = J / columnBlockSize;
            numberOfBlockRowsAnotherMatrix = K / rowBlockSize;
        } else {
            numberOfBlockColumns = J / columnBlockSize;
            numberOfBlockRows = K / rowBlockSize;
            numberOfBlockColumnsAnotherMatrix = K / columnBlockSize;
            numberOfBlockRowsAnotherMatrix = I / rowBlockSize;
        }
        String stringRow = value.toString();
        //System.out.println(stringRow);
        String[] rowValues = stringRow.split("\\s+");
        long rowIndex = Long.parseLong(rowValues[0]);
        long columnIndex = Long.parseLong(rowValues[1]);
        double matrixValue = Double.parseDouble(rowValues[2]);
        LongWritable rowBlockIndex = new LongWritable(rowIndex/rowBlockSize);
        LongWritable columnBlockIndex = new LongWritable(columnIndex/columnBlockSize);
        LongWritable inBlockRowIndex = new LongWritable(rowIndex%rowBlockSize);
        LongWritable inBlockColumnIndex = new LongWritable(columnIndex%columnBlockSize);
        if (matrixIntNumber == 0) {
            for (long copy = 0; copy < numberOfBlockColumnsAnotherMatrix;copy++) {
                context.write(new ComplexKey(rowBlockIndex,columnBlockIndex,new LongWritable(copy)),
                        new ComplexValue(inBlockRowIndex,inBlockColumnIndex,new DoubleWritable(matrixValue),matrixNumber));
                //System.out.println(rowBlockIndex.get()+", "+columnBlockIndex.get()+", "+copy+"\t"+inBlockRowIndex.get() +", "+ inBlockColumnIndex.get() + ", " + matrixValue + ", " + matrixIntNumber);
            }
        } else {
            for (long copy = 0; copy < numberOfBlockRowsAnotherMatrix;copy++) {
                context.write(new ComplexKey(new LongWritable(copy),rowBlockIndex,columnBlockIndex),
                        new ComplexValue(inBlockRowIndex,inBlockColumnIndex,new DoubleWritable(matrixValue),matrixNumber));
                //System.out.println(copy + ", " + rowBlockIndex.get() + ", " + columnBlockIndex.get() + "\t"+inBlockRowIndex.get() +", "+ inBlockColumnIndex.get() + ", " + matrixValue + ", " + matrixIntNumber);
            }
        }
    }
    private int getMatrixNumber(Context context) {
        InputSplit split = context.getInputSplit();
        String fileName = ((FileSplit) split).getPath().getName();
        if (fileName.contains("B")) {
            return 1;
        } else {
            return 0;
        }
    }
}
/*
class MyPartitioner extends Partitioner<ComplexKey,ComplexValue> {
    @Override
    public int getPartition(ComplexKey complexKey, ComplexValue complexValue, int i) {
        ComplexKey newKey= new ComplexKey(complexKey);
        newKey.setMatrixNumber(0);
        return newKey.hashCode();
    }
}*/

class MatrixCoordinates implements WritableComparable<MatrixCoordinates> {
    private LongWritable row;
    private LongWritable column;

    public MatrixCoordinates() {
        row = new LongWritable();
        column = new LongWritable();
    }

    @Override
    public int hashCode() {
        return  new String(row.get() + " " + column.get()).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MatrixCoordinates)) {
            return false;
        }
        return row.get() == (((MatrixCoordinates) obj).row.get()) &&
                (column.get() == ((MatrixCoordinates) obj).column.get());
    }

    @Override
    public String toString() {
        return row.get() + " " + column.get();
    }

    public MatrixCoordinates(LongWritable newRow, LongWritable newColumn) {
        this();
        row.set(newRow.get());
        column.set(newColumn.get());
    }

    @Override
    public int compareTo(MatrixCoordinates o) {
        int rowResult = row.compareTo(o.row);
        int columnResult = column.compareTo(o.column);
        if (rowResult == 0) {
            return columnResult;
        } else {
            return rowResult;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        row.write(dataOutput);
        column.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        row.readFields(dataInput);
        column.readFields(dataInput);
    }
}

class MyReducer extends Reducer<ComplexKey, ComplexValue,MatrixCoordinates, DoubleWritable > {
    @Override
    protected void reduce(ComplexKey key, Iterable<ComplexValue> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        long rowBlockSize = Long.parseLong(conf.get("BlockI"));
        long columnBlockSize = Long.parseLong(conf.get("BlockJ"));
        double[][] blockA = new double[(int)rowBlockSize][(int)columnBlockSize];
        double[][] blockB = new double[(int)rowBlockSize][(int)columnBlockSize];
        long rowOffsetA = key.getRowBlockIndex()*rowBlockSize;
        long columnOffsetB = key.getNumberOfCopy()*columnBlockSize;
        for (ComplexValue value : values) {
            int matrixNumber = value.getMatrixNumber();
            int i = (int) value.getInBlockRowIndex().get();
            int j = (int) value.getInBlockColumnIndex().get();
            double matrixValue = value.getValue();
            if (matrixNumber == 0) {
                blockA[i][j] = matrixValue;
            } else {
                blockB[i][j] = matrixValue;
            }
        }

        for (int i = 0; i < rowBlockSize; i++) {
            for (int j = 0; j < columnBlockSize; j++) {
                double cElementPart = 0;
                for (int k = 0;k < columnBlockSize;k++) {
                    cElementPart+= blockA[i][k]*blockB[k][j];
                }
                context.write(new MatrixCoordinates(new LongWritable(rowOffsetA+i),new LongWritable(columnOffsetB+j)),
                        new DoubleWritable(cElementPart));

            }
        }

    }
}

class YetAnotherMapper extends Mapper<Object,Text, MatrixCoordinates,DoubleWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String stringValue = value.toString();
        String[] parsedValues = stringValue.split("\\s+");
        long row = Long.parseLong(parsedValues[0]);
        long column = Long.parseLong(parsedValues[1]);
        double matrixValue = Double.parseDouble(parsedValues[2]);
        context.write(new MatrixCoordinates(new LongWritable(row),new LongWritable(column)),new DoubleWritable(matrixValue));
    }
}

class YetAnotherReducer extends Reducer<MatrixCoordinates,DoubleWritable,MatrixCoordinates,DoubleWritable> {

    @Override
    protected void reduce(MatrixCoordinates key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
        }
        context.write(key,new DoubleWritable(sum));
    }
}

public class MatrixMultiplication {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("I",args[2]); // number of rows in matrix A
        conf.set("K", args[3]); // number of columns in matrix A = number of rows in matrix B
        conf.set("J", args[4]); // number of columns in matrix B
        conf.set("BlockI", args[5]); // number of rows in block
        conf.set("BlockJ", args[6]); // number of columns in block;
        Job job = Job.getInstance( conf, "multBlocks");
        Job secondJob = Job.getInstance(new Configuration(), "sum");
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        secondJob.setMapperClass(YetAnotherMapper.class);
        secondJob.setReducerClass(YetAnotherReducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("tmp"));
        FileInputFormat.addInputPath(secondJob, new Path("tmp"));
        FileOutputFormat.setOutputPath(secondJob, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        secondJob.setInputFormatClass(TextInputFormat.class);
        secondJob.setOutputFormatClass(TextOutputFormat.class);
        secondJob.setOutputKeyClass(MatrixCoordinates.class);
        secondJob.setOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(ComplexKey.class);
        job.setOutputValueClass(ComplexValue.class);
        job.setJarByClass(MatrixMultiplication.class);
        secondJob.setJarByClass(MatrixMultiplication.class);
        job.waitForCompletion(false);
        secondJob.waitForCompletion(false);
    }

}
