import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Employee implements Writable {
    public String name;
    public int dno;
    public String address;

    Employee () {}

    Employee ( String n, int d, String a ) {
        name = n; dno = d; address = a;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(dno);
        out.writeUTF(name);
        out.writeUTF(address);
    }

    public void readFields ( DataInput in ) throws IOException {
        dno = in.readInt();
        name = in.readUTF();
        address = in.readUTF();
    }
}

class Department implements Writable {
    public String name;
    public int dno;

    Department () {}

    Department ( String n, int d ) {
        name = n; dno = d;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(dno);
        out.writeUTF(name);
    }

    public void readFields ( DataInput in ) throws IOException {
        dno = in.readInt();
        name = in.readUTF();
    }
}

class Result implements Writable {
    public String ename;
    public String dname;

    Result ( String en, String dn ) {
        ename = en; dname = dn;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeUTF(ename);
        out.writeUTF(dname);
    }

    public void readFields ( DataInput in ) throws IOException {
        ename = in.readUTF();
        dname = in.readUTF();
    }

    public String toString () { return ename+" "+dname; }
}

class EmpDept implements Writable {
    public short tag;
    public Employee employee;
    public Department department;

    EmpDept () {}
    EmpDept ( Employee e ) { tag = 0; employee = e; }
    EmpDept ( Department d ) { tag = 1; department = d; }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        if (tag==0)
            employee.write(out);
        else department.write(out);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        if (tag==0) {
            employee = new Employee();
            employee.readFields(in);
        } else {
            department = new Department();
            department.readFields(in);
        }
    }
}

public class Join {
    public static class EmployeeMapper extends Mapper<Object,Text,IntWritable,EmpDept > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            Employee e = new Employee(s.next(),s.nextInt(),s.next());
            context.write(new IntWritable(e.dno),new EmpDept(e));
            s.close();
        }
    }

    public static class DepartmentMapper extends Mapper<Object,Text,IntWritable,EmpDept > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            Department d = new Department(s.next(),s.nextInt());
            context.write(new IntWritable(d.dno),new EmpDept(d));
            s.close();
        }
    }

    public static class ResultReducer extends Reducer<IntWritable,EmpDept,IntWritable,Result> {
        static Vector<Employee> emps = new Vector<Employee>();
        static Vector<Department> depts = new Vector<Department>();
        @Override
        public void reduce ( IntWritable key, Iterable<EmpDept> values, Context context )
                           throws IOException, InterruptedException {
            emps.clear();
            depts.clear();
            for (EmpDept v: values)
                if (v.tag == 0)
                    emps.add(v.employee);
                else depts.add(v.department);
            for ( Employee e: emps )
                for ( Department d: depts )
                    context.write(key,new Result(e.name,d.name));
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("JoinJob");
        job.setJarByClass(Join.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Result.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(EmpDept.class);
        job.setReducerClass(ResultReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,EmployeeMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,DepartmentMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
