from qtextasdata.core import DataStream, QInputParams, QTextAsData
from test.base import AbstractQTestCase


import six
from test.utils import DEBUG
import sys
import codecs
from test.test_data import header_row_with_spaces, sample_data_no_header, sample_data_with_header
from test.base import AbstractQTestCase

class BasicModuleTests(AbstractQTestCase):

    def test_engine_isolation(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("d e f\n10 20 30\n40 50 60"))

        # Run file 1 on engine 1
        q1 = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r = q1.execute('select * from %s' % tmpfile1.name)
        print("QueryQuery",file=sys.stdout)

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r.data,[(1,2,3),(4,5,6)])
        self.assertTrue(tmpfile1.name in r.metadata.table_structures)
        self.assertTrue(tmpfile1.name in r.metadata.new_table_structures)
        self.assertEqual(r.metadata.table_structures[tmpfile1.name].atomic_fns,[tmpfile1.name])
        self.assertEqual(r.metadata.table_structures[tmpfile1.name].source_type,'file')
        self.assertEqual(r.metadata.table_structures[tmpfile1.name].source,tmpfile1.name)

        # run file 1 on engine 2
        q2 = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r2 = q2.execute('select * from %s' % tmpfile1.name)
        print("QueryQuery",file=sys.stdout)

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r2.warnings),0)
        self.assertEqual(len(r2.data),2)
        self.assertEqual(r2.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r2.data,[(1,2,3),(4,5,6)])
        self.assertTrue(tmpfile1.name in r2.metadata.table_structures)
        self.assertTrue(tmpfile1.name in r2.metadata.new_table_structures)
        self.assertEqual(r2.metadata.table_structures[tmpfile1.name].atomic_fns,[tmpfile1.name])
        self.assertEqual(r2.metadata.table_structures[tmpfile1.name].source_type,'file')
        self.assertEqual(r2.metadata.table_structures[tmpfile1.name].source,tmpfile1.name)

        # run file 2 on engine 1
        r3 = q1.execute('select * from %s' % tmpfile2.name)
        print("QueryQuery",file=sys.stdout)

        print(r3)
        self.assertTrue(r3.status == 'ok')
        self.assertEqual(len(r3.warnings),0)
        self.assertEqual(len(r3.data),2)
        self.assertEqual(r3.metadata.output_column_name_list,['d','e','f'])
        self.assertEqual(r3.data,[(10,20,30),(40,50,60)])
        self.assertTrue(tmpfile2.name in r3.metadata.table_structures)
        self.assertTrue(tmpfile2.name in r3.metadata.new_table_structures)
        self.assertEqual(r3.metadata.table_structures[tmpfile2.name].atomic_fns,[tmpfile2.name])
        self.assertEqual(r3.metadata.table_structures[tmpfile2.name].source,tmpfile2.name)
        self.assertEqual(r3.metadata.table_structures[tmpfile2.name].source_type,'file')

        q1.done()
        q2.done()

        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_simple_query(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r = q.execute('select * from %s' % tmpfile.name)

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r.data,[(1,2,3),(4,5,6)])
        self.assertTrue(tmpfile.name in r.metadata.table_structures)
        self.assertTrue(tmpfile.name in r.metadata.new_table_structures)
        self.assertEqual(r.metadata.table_structures[tmpfile.name].atomic_fns,[tmpfile.name])
        self.assertEqual(r.metadata.table_structures[tmpfile.name].source_type,'file')
        self.assertEqual(r.metadata.table_structures[tmpfile.name].source,tmpfile.name)

        q.done()
        self.cleanup(tmpfile)

    def test_loaded_data_reuse(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))
        r1 = q.execute('select * from %s' % tmpfile.name)

        r2 = q.execute('select * from %s' % tmpfile.name)

        self.assertTrue(r1.status == 'ok')
        self.assertEqual(len(r1.warnings),0)
        self.assertEqual(len(r1.data),2)
        self.assertEqual(r1.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r1.data,[(1,2,3),(4,5,6)])
        self.assertTrue(tmpfile.name in r1.metadata.table_structures)
        self.assertTrue(tmpfile.name in r1.metadata.new_table_structures)
        self.assertEqual(r1.metadata.table_structures[tmpfile.name].atomic_fns,[tmpfile.name])
        self.assertEqual(r1.metadata.table_structures[tmpfile.name].source_type,'file')
        self.assertEqual(r1.metadata.table_structures[tmpfile.name].source,tmpfile.name)

        self.assertTrue(r2.status == 'ok')
        self.assertTrue(tmpfile.name in r2.metadata.table_structures)
        self.assertTrue(tmpfile.name not in r2.metadata.new_table_structures)
        self.assertEqual(r2.data,r1.data)
        self.assertEqual(r2.metadata.output_column_name_list,r2.metadata.output_column_name_list)
        self.assertEqual(len(r2.warnings),0)

        q.done()

        self.cleanup(tmpfile)

    def test_stdin_injection(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        data_streams_dict = {
            '-': DataStream('stdin','-',codecs.open(tmpfile.name,'rb',encoding='utf-8'))
        }
        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '),data_streams_dict=data_streams_dict)
        r = q.execute('select * from -')

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r.data,[(1,2,3),(4,5,6)])
        self.assertEqual(r.metadata.new_table_structures['-'],r.metadata.table_structures['-'])
        self.assertEqual(r.metadata.table_structures['-'].column_names,['a','b','c'])
        self.assertEqual(r.metadata.table_structures['-'].python_column_types,[int,int,int])
        self.assertEqual(r.metadata.table_structures['-'].sqlite_column_types,['int','int','int'])
        self.assertEqual(r.metadata.table_structures['-'].source_type,'data-stream')
        self.assertEqual(r.metadata.table_structures['-'].source,'stdin')

        q.done()
        self.cleanup(tmpfile)

    def test_named_stdin_injection(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        data_streams_dict = {
            'my_stdin_data': DataStream('my_stdin_data','my_stdin_data',codecs.open(tmpfile.name,'rb',encoding='utf-8'))
        }

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '),data_streams_dict=data_streams_dict)
        r = q.execute('select a from my_stdin_data')

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),2)
        self.assertEqual(r.metadata.output_column_name_list,['a'])
        self.assertEqual(r.data,[(1,),(4,)])
        self.assertTrue('my_stdin_data' in r.metadata.table_structures)
        self.assertTrue('my_stdin_data' in r.metadata.new_table_structures)
        self.assertEqual(r.metadata.table_structures['my_stdin_data'].qtable_name,'my_stdin_data')

        q.done()
        self.cleanup(tmpfile)

    def test_data_stream_isolation(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("d e f\n7 8 9\n10 11 12"))

        data_streams_dict = {
            'a-': DataStream('a-','a-',codecs.open(tmpfile1.name, 'rb', encoding='utf-8')),
            'b-': DataStream('b-','b-',codecs.open(tmpfile2.name, 'rb', encoding='utf-8'))
        }

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '),data_streams_dict=data_streams_dict)
        r1 = q.execute('select * from a-')

        self.assertTrue(r1.status == 'ok')
        self.assertEqual(len(r1.warnings),0)
        self.assertEqual(len(r1.data),2)
        self.assertEqual(r1.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r1.data,[(1,2,3),(4,5,6)])
        self.assertTrue('a-' in r1.metadata.table_structures)
        self.assertEqual(len(r1.metadata.table_structures),1)
        self.assertEqual(r1.metadata.table_structures['a-'].source_type, 'data-stream')
        self.assertEqual(r1.metadata.table_structures['a-'].source, 'a-')
        self.assertEqual(r1.metadata.table_structures['a-'].column_names, ['a','b','c'])
        self.assertEqual(r1.metadata.table_structures['a-'].python_column_types, [int,int,int])
        self.assertEqual(r1.metadata.table_structures['a-'].sqlite_column_types, ['int','int','int'])

        r2 = q.execute('select * from b-')

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r2.warnings),0)
        self.assertEqual(len(r2.data),2)
        self.assertEqual(r2.metadata.output_column_name_list,['d','e','f'])
        self.assertEqual(r2.data,[(7,8,9),(10,11,12)])

        self.assertEqual(len(r1.metadata.table_structures),2)
        self.assertTrue('b-' in r1.metadata.table_structures)
        self.assertEqual(r1.metadata.table_structures['b-'].source_type, 'data-stream')
        self.assertEqual(r1.metadata.table_structures['b-'].source, 'b-')
        self.assertEqual(r1.metadata.table_structures['b-'].column_names, ['d','e','f'])
        self.assertEqual(r1.metadata.table_structures['b-'].python_column_types, [int,int,int])
        self.assertEqual(r1.metadata.table_structures['b-'].sqlite_column_types, ['int','int','int'])

        q.done()
        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_multiple_stdin_injection(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("d e f\n7 8 9\n10 11 12"))

        data_streams_dict = {
            'my_stdin_data1': DataStream('my_stdin_data1','my_stdin_data1',codecs.open(tmpfile1.name,'rb',encoding='utf-8')),
            'my_stdin_data2': DataStream('my_stdin_data2','my_stdin_data2',codecs.open(tmpfile2.name,'rb',encoding='utf-8'))
        }
        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '),data_streams_dict=data_streams_dict)
        r1 = q.execute('select * from my_stdin_data1')

        self.assertTrue(r1.status == 'ok')
        self.assertEqual(len(r1.warnings),0)
        self.assertEqual(len(r1.data),2)
        self.assertEqual(r1.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r1.data,[(1,2,3),(4,5,6)])
        self.assertTrue('my_stdin_data1' in r1.metadata.table_structures)
        self.assertTrue('my_stdin_data1' in r1.metadata.new_table_structures)
        self.assertEqual(r1.metadata.table_structures['my_stdin_data1'].qtable_name,'my_stdin_data1')

        r2 = q.execute('select * from my_stdin_data2')

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r2.warnings),0)
        self.assertEqual(len(r2.data),2)
        self.assertEqual(r2.metadata.output_column_name_list,['d','e','f'])
        self.assertEqual(r2.data,[(7,8,9),(10,11,12)])
        # There should be another data load, even though it's the same 'filename' as before
        self.assertTrue('my_stdin_data2' in r2.metadata.table_structures)
        self.assertTrue('my_stdin_data2' in r2.metadata.new_table_structures)
        self.assertEqual(r2.metadata.table_structures['my_stdin_data2'].qtable_name,'my_stdin_data2')

        r3 = q.execute('select aa.*,bb.* from my_stdin_data1 aa join my_stdin_data2 bb')

        self.assertTrue(r3.status == 'ok')
        self.assertEqual(len(r3.warnings),0)
        self.assertEqual(len(r3.data),4)
        self.assertEqual(r3.metadata.output_column_name_list,['a','b','c','d','e','f'])
        self.assertEqual(r3.data,[(1,2,3,7,8,9),(1,2,3,10,11,12),(4,5,6,7,8,9),(4,5,6,10,11,12)])
        self.assertTrue('my_stdin_data1' in r3.metadata.table_structures)
        self.assertTrue('my_stdin_data1' not in r3.metadata.new_table_structures)

        q.done()
        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_different_input_params_for_different_files(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("7\t8\t9\n10\t11\t12"))

        q = QTextAsData(QInputParams(skip_header=True,delimiter=' '))

        q.load_data(tmpfile1.name,QInputParams(skip_header=True,delimiter=' '))
        q.load_data(tmpfile2.name,QInputParams(skip_header=False,delimiter='\t'))

        r = q.execute('select aa.*,bb.* from %s aa join %s bb' % (tmpfile1.name,tmpfile2.name))

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),4)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c','c1','c2','c3'])
        self.assertEqual(r.data,[(1,2,3,7,8,9),(1,2,3,10,11,12),(4,5,6,7,8,9),(4,5,6,10,11,12)])
        self.assertTrue(tmpfile1.name not in r.metadata.new_table_structures)
        self.assertTrue(tmpfile2.name not in r.metadata.new_table_structures)

        q.done()
        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_different_input_params_for_different_files_2(self):
        tmpfile1 = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))
        tmpfile2 = self.create_file_with_data(six.b("7\t8\t9\n10\t11\t12"))

        q = QTextAsData()

        q.load_data(tmpfile1.name,QInputParams(skip_header=True,delimiter=' '))
        q.load_data(tmpfile2.name,QInputParams(skip_header=False,delimiter='\t'))

        r = q.execute('select aa.*,bb.* from %s aa join %s bb' % (tmpfile1.name,tmpfile2.name))

        self.assertTrue(r.status == 'ok')
        self.assertEqual(len(r.warnings),0)
        self.assertEqual(len(r.data),4)
        self.assertEqual(r.metadata.output_column_name_list,['a','b','c','c1','c2','c3'])
        self.assertEqual(r.data,[(1,2,3,7,8,9),(1,2,3,10,11,12),(4,5,6,7,8,9),(4,5,6,10,11,12)])
        self.assertTrue(tmpfile1.name not in r.metadata.new_table_structures)
        self.assertTrue(tmpfile2.name not in r.metadata.new_table_structures)

        q.done()
        self.cleanup(tmpfile1)
        self.cleanup(tmpfile2)

    def test_input_params_override(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        default_input_params = QInputParams()

        for k in default_input_params.__dict__.keys():
            setattr(default_input_params,k,'GARBAGE')

        q = QTextAsData(default_input_params)

        r = q.execute('select * from %s' % tmpfile.name)

        self.assertTrue(r.status == 'error')

        overwriting_input_params = QInputParams(skip_header=True,delimiter=' ')

        r2 = q.execute('select * from %s' % tmpfile.name,input_params=overwriting_input_params)

        self.assertTrue(r2.status == 'ok')
        self.assertEqual(len(r2.warnings),0)
        self.assertEqual(len(r2.data),2)
        self.assertEqual(r2.metadata.output_column_name_list,['a','b','c'])
        self.assertEqual(r2.data,[(1,2,3),(4,5,6)])
        self.assertTrue(tmpfile.name in r2.metadata.table_structures)
        self.assertTrue(tmpfile.name in r2.metadata.new_table_structures)
        self.assertEqual(r2.metadata.table_structures[tmpfile.name].atomic_fns,[tmpfile.name])
        self.assertEqual(r2.metadata.table_structures[tmpfile.name].source,tmpfile.name)
        self.assertEqual(r2.metadata.table_structures[tmpfile.name].source_type,'file')

        q.done()
        self.cleanup(tmpfile)

    def test_input_params_merge(self):
        input_params = QInputParams()

        for k in input_params.__dict__.keys():
            setattr(input_params,k,'GARBAGE')

        merged_input_params = input_params.merged_with(QInputParams())

        for k in merged_input_params.__dict__.keys():
            self.assertTrue(getattr(merged_input_params,k) != 'GARBAGE')

        for k in input_params.__dict__.keys():
            self.assertTrue(getattr(merged_input_params,k) != 'GARBAGE')

    def test_table_analysis_with_syntax_error(self):

        q = QTextAsData()

        q_output = q.analyze("bad syntax")

        q.done()
        self.assertTrue(q_output.status == 'error')
        self.assertTrue(q_output.error.msg.startswith('query error'))

    def test_execute_response(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData()

        q_output = q.execute("select a,c from %s" % tmpfile.name,QInputParams(skip_header=True))

        self.assertTrue(q_output.status == 'ok')
        self.assertTrue(q_output.error is None)
        self.assertEqual(len(q_output.warnings),0)
        self.assertEqual(len(q_output.data),2)
        self.assertEqual(q_output.data,[ (1,3),(4,6) ])
        self.assertTrue(q_output.metadata is not None)

        metadata = q_output.metadata

        self.assertEqual(metadata.output_column_name_list, [ 'a','c'])
        self.assertTrue(tmpfile.name in metadata.new_table_structures)
        self.assertEqual(len(metadata.table_structures),1)

        table_structure = metadata.new_table_structures[tmpfile.name]

        self.assertEqual(table_structure.column_names,[ 'a','b','c'])
        self.assertEqual(table_structure.python_column_types,[ int,int,int])
        self.assertEqual(table_structure.sqlite_column_types,[ 'int','int','int'])
        self.assertEqual(table_structure.qtable_name, tmpfile.name)
        self.assertEqual(table_structure.atomic_fns,[tmpfile.name])
        self.assertEqual(table_structure.source_type,'file')
        self.assertEqual(table_structure.source,tmpfile.name)

        q.done()
        self.cleanup(tmpfile)

    def test_analyze_response(self):
        tmpfile = self.create_file_with_data(six.b("a b c\n1 2 3\n4 5 6"))

        q = QTextAsData()

        q_output = q.analyze("select a,c from %s" % tmpfile.name,QInputParams(skip_header=True))

        self.assertTrue(q_output.status == 'ok')
        self.assertTrue(q_output.error is None)
        self.assertEqual(len(q_output.warnings),0)
        self.assertEqual(len(q_output.data),2)
        self.assertEqual(q_output.data,[ (1,3),(4,6) ])
        self.assertTrue(q_output.metadata is not None)

        metadata = q_output.metadata

        self.assertEqual(metadata.output_column_name_list, [ 'a','c'])
        self.assertEqual(len(metadata.table_structures),1)
        self.assertTrue(tmpfile.name in metadata.new_table_structures)

        table_structure = metadata.table_structures[tmpfile.name]

        self.assertEqual(table_structure.column_names,[ 'a','b','c'])
        self.assertEqual(table_structure.python_column_types,[ int,int,int])
        self.assertEqual(table_structure.sqlite_column_types,[ 'int','int','int'])
        self.assertEqual(table_structure.qtable_name, tmpfile.name)
        self.assertEqual(table_structure.atomic_fns,[tmpfile.name])
        self.assertEqual(table_structure.source_type,'file')
        self.assertEqual(table_structure.source,tmpfile.name)

        q.done()
        self.cleanup(tmpfile)

    def test_load_data_from_string_without_previous_data_load(self):
        input_str = six.u('column1,column2,column3\n') + six.u('\n').join([six.u('value1,2.5,value3')] * 1000)


        data_streams_dict = {
            'my_data': DataStream('my_data_stream_id','my_data',six.StringIO(input_str))
        }
        q = QTextAsData(default_input_params=QInputParams(skip_header=True,delimiter=','),data_streams_dict=data_streams_dict)

        q_output = q.execute('select column2,column3 from my_data')

        self.assertTrue(q_output.status == 'ok')
        self.assertTrue(q_output.error is None)
        self.assertEqual(len(q_output.warnings),0)
        self.assertTrue(len(q_output.data),1000)
        self.assertEqual(len(set(q_output.data)),1)
        self.assertEqual(list(set(q_output.data))[0],(2.5,'value3'))

        metadata = q_output.metadata

        self.assertTrue(metadata.output_column_name_list,['column2','column3'])
        self.assertTrue('my_data' in metadata.new_table_structures)
        self.assertEqual(len(metadata.table_structures),1)

        table_structure = metadata.table_structures['my_data']

        self.assertEqual(table_structure.column_names,['column1','column2','column3'])
        self.assertEqual(table_structure.sqlite_column_types,['text','real','text'])
        self.assertEqual(table_structure.python_column_types,[str,float,str])
        self.assertEqual(table_structure.qtable_name, 'my_data')
        self.assertEqual(table_structure.source_type, 'data-stream')
        self.assertEqual(table_structure.source, 'my_data_stream_id')

        q.done()

    def test_load_data_from_string_with_previous_data_load(self):
        input_str = six.u('column1,column2,column3\n') + six.u('\n').join([six.u('value1,2.5,value3')] * 1000)

        data_streams_dict = {
            'my_data': DataStream('a','my_data',six.StringIO(input_str))
        }
        q = QTextAsData(default_input_params=QInputParams(skip_header=True,delimiter=','),data_streams_dict=data_streams_dict)

        dl = q.load_data('my_data',QInputParams(skip_header=True,delimiter=','))

        q_output = q.execute('select column2,column3 from my_data')

        self.assertTrue(q_output.status == 'ok')
        self.assertTrue(q_output.error is None)
        self.assertEqual(len(q_output.warnings),0)
        self.assertTrue(len(q_output.data),1000)
        self.assertEqual(len(set(q_output.data)),1)
        self.assertEqual(list(set(q_output.data))[0],(2.5,'value3'))

        metadata = q_output.metadata

        self.assertTrue(metadata.output_column_name_list,['column2','column3'])
        self.assertTrue('my_data' not in metadata.new_table_structures)
        self.assertEqual(len(metadata.table_structures),1)

        table_structure = metadata.table_structures['my_data']

        self.assertEqual(table_structure.column_names,['column1','column2','column3'])
        self.assertEqual(table_structure.sqlite_column_types,['text','real','text'])
        self.assertEqual(table_structure.python_column_types,[str,float,str])
        self.assertEqual(table_structure.qtable_name, 'my_data')

        q.done()