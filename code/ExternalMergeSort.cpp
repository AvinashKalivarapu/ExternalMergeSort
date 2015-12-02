
#include<bits/stdc++.h>

using namespace std;

//global variables
bool sort_order=true;

//structure definations
typedef struct pointer_store{
	vector< string > data;
	long long int fp;
	long long int steps;
	long long int total;
} pstore;

struct comp{
	inline bool operator() (const pstore& a, const pstore& b)
	{
		for(long long int i=0;i<a.data.size();i++)
		{
			if(a.data[i].compare(b.data[i])<0)
			{
				if(sort_order)
					return false;
				else
					return true;
			}
			else if(a.data[i].compare(b.data[i])>0)
			{
				if(sort_order)
					return true;
				else
					return false;
			}
		}
		if(sort_order)
		return true;
		else 
			return false;
	}
};

//function declarations
void read_metadata(map< string, long long int > & colname_to_index,long long int & row_mem,long long int & total_columns); //read metadata
void print_page(vector< vector < string > > & page, vector < long long int > & reverse_order,ofstream & out_stream);
void page_sorter(long long int & total_rows,long long int & page_size,long long int & total_columns,vector< long long int > & preference,vector < long long int > & reverse_order,string inputfile,string outputfile); //to sort individual page after reading
void sort_by_pointer(long long int & p_counter,long long int & total_pointers,long long int & page_size,long long int & total_columns,vector< long long int > & preference,vector< long long int > & reverse_order,string inputfile,string outputfile, long long int & g_memory,long long int & row_mem);
void sort_by_heap(ofstream & outstream, long long int & page_size,long long int & total_columns, long long int & row_mem,string inputfile,string outputfile,vector< pstore > & all_pointers,vector< long long int > & preference, vector< long long int > & reverse_order);
void print_all_pointers_data(vector< pstore > & all_pointers);
void generate_output(string infile,string outfile,vector < long long int > & reverse_order);

void print_all_pointers_data(vector< pstore > & all_pointers)
{
	for(long long int i=0;i<all_pointers.size();i++)
	{
		cout<<i<<": ";
		for(long long int j=0;j<all_pointers[i].data.size();j++)
			cout<<all_pointers[i].data[j]<<" ";
		cout<<endl;
	}
	return;
}

void sort_by_heap(ofstream & outstream,long long int & page_size,long long int & total_columns, long long int & row_mem,string inputfile,string outputfile,vector< pstore > & all_pointers,vector< long long int > & preference, vector< long long int > & reverse_order)
{
	if(!is_heap(all_pointers.begin(),all_pointers.end(),comp()))
		make_heap(all_pointers.begin(),all_pointers.end(),comp());
	pstore current;
	string line,token;
	vector< string > data;
	long long int count=0;
	long long int curr_bytes;
	ifstream tempstream(inputfile);
	while(all_pointers.size()>0)
	{
		current=all_pointers.front();
		pop_heap(all_pointers.begin(),all_pointers.end(),comp());
		all_pointers.pop_back();
		for(long long int i=0;i<current.data.size();i++)
		{
			outstream<<current.data[i]<<" ";
		}
		outstream<<endl;
		if(current.steps<current.total)
		{
			curr_bytes=(((current.fp-1)*page_size)+current.steps)*(row_mem+total_columns+1);
			tempstream.seekg(curr_bytes);
			if(!getline(tempstream,line))
				continue;
			stringstream linestream(line);
			while(linestream>>token)
			{
				data.push_back(token);
			}
			current.data=data;
			current.steps++;
			all_pointers.push_back(current);
			data.clear();
			push_heap(all_pointers.begin(),all_pointers.end(),comp());
		}
		count++;
	}
	tempstream.close();
	return;
}

void sort_by_pointer(long long int & p_counter,long long int & total_pointers,long long int & page_size,long long int & total_columns,vector< long long int > & preference,vector< long long int > & reverse_order,string inputfile,string outputfile, long long int & g_memory,long long int & row_mem)
{
	vector < pstore > all_pointers;
	all_pointers.clear();
	pstore p_cand;
	vector< string > tempdata,data;
	vector< vector< string > > page;
	string token,line;
	long long int counter=page_size;
	p_counter=0;
	long long int t_p_counter=0;
	long long int t_counter=0;
	long long int prev_count;
	ifstream inputstream(inputfile);
	ofstream outstream;
	outstream.clear();
	outstream.open(outputfile);
	while(getline(inputstream,line))
	{
		if(p_counter==total_pointers)
		{
			sort_by_heap(outstream,page_size,total_columns,row_mem,inputfile,outputfile,all_pointers,preference,reverse_order);
			p_counter=0;
			all_pointers.clear();
		}
		if((counter%page_size)==0)
		{
			stringstream linestream(line);
			while(linestream>>token)
			{
				data.push_back(token);
			}
			p_counter++;
			t_p_counter++;
			p_cand.data=data;
			p_cand.fp=(t_counter/page_size) + 1;
			p_cand.steps=1;
			p_cand.total=counter;
			prev_count=t_counter;
			all_pointers.push_back(p_cand);
			counter=0;
		}
		data.clear();
		counter++;
		t_counter++;
	}
	if(p_counter>0)
	{
		all_pointers[all_pointers.size()-1].total=t_counter%page_size;
		sort_by_heap(outstream,page_size,total_columns,row_mem,inputfile,outputfile,all_pointers,preference,reverse_order);
	}
	p_counter=t_p_counter; //new
	inputstream.close();
	outstream.close();
	return;
}

int main( int argc, char * argv[] )
{
	long long int row_mem;
	vector< string > colorder;
	string inputfile=argv[1];
	string outputfile=argv[2];
	long long int g_memory=(long long int)(0.8*(double)(1024*1024*atoi(argv[3])));
	sort_order=(argv[4][0]=='a'?true:false);
	long long int total_columns=0;
	for(int i=5;i<argc;i++)
		colorder.push_back(argv[i]);
	map < string, long long int > colname_to_index;
	read_metadata(colname_to_index,row_mem,total_columns);
	set< long long int > repeats;
	vector < long long int > reverse_order;
	reverse_order.clear();
	reverse_order.resize(total_columns);
	vector < long long int > preference;
	preference.clear();
	for(long long int i=0;i<colorder.size();i++)
	{
		preference.push_back(colname_to_index[colorder[i]]);
		repeats.insert(colname_to_index[colorder[i]]);
	}
	for(long long int i=0;i<total_columns;i++)
	{
		if(repeats.find(i)==repeats.end())
		{
			preference.push_back(i);
		}
	}
	for(long long int i=0;i<total_columns;i++)
	{
		reverse_order[preference[i]]=i;
	}
	g_memory=0.6*g_memory; //so that it never exceeds
	long long int page_size=(long long int)((double)g_memory/(double)row_mem);
	long long int total_rows=0;
	page_sorter(total_rows,page_size,total_columns,preference,reverse_order,inputfile,"temp1.txt");
	bool flag=0;
	long long int total_pointers=(g_memory/(24+row_mem))-1;
	long long int p_counter=INT_MAX;
	if(page_size==0 || total_pointers==0)
	{
		cout<<"Not feasible for given memory\n";
		return 0;
	}
	while(p_counter>total_pointers)
	{
		if(flag==0)
		{
			sort_by_pointer(p_counter,total_pointers,page_size,total_columns,preference,reverse_order,"temp1.txt","temp2.txt",g_memory,row_mem);
			flag=1;
		}
		else
		{
			sort_by_pointer(p_counter,total_pointers,page_size,total_columns,preference,reverse_order,"temp2.txt","temp1.txt",g_memory,row_mem);
			flag=0;
		}
		page_size=page_size*(total_pointers);
	}
	if(flag)
		generate_output("temp2.txt",outputfile,reverse_order);
	else
		generate_output("temp1.txt",outputfile,reverse_order);

		return 0;
}

void generate_output(string infile,string outfile,vector < long long int > & reverse_order)
{
	vector< string > data;
	ifstream instream(infile);
	ofstream outstream(outfile);
	string line,token;
	while(getline(instream,line))
	{
		data.clear();
		stringstream linestream(line);
		while(linestream>>token)
		{
			data.push_back(token);
		}
		for(long long int i=0;i<data.size();i++)
		{
			outstream<< data[reverse_order[i]]<< " "; 
		}
		outstream<<endl;
	}
	instream.close();
	outstream.close();
	return;
}

void read_metadata(map< string, long long int > & colname_to_index,long long int & row_mem,long long int & total_columns)
{
	ifstream mstream("metadata.txt");
	string metadata;
	long long int counter=0;
	size_t compos;
	row_mem=0;
	while(mstream>>metadata)
	{
		compos=metadata.find(",");
		colname_to_index[metadata.substr(0,compos)]=counter;
		counter++;
		total_columns++;
		row_mem=row_mem+(float)atoi(metadata.substr(compos+1).c_str());
	}
	mstream.close();
	return;
}

void print_page(vector< vector < string > > & page, vector < long long int > & reverse_order,ofstream & out_stream)
{
	if(sort_order)
		sort(page.begin(),page.end());
	else
		sort(page.begin(),page.end(),greater< vector< string > >());
	for( long long int i=0;i<page.size();i++)
	{
		for (long long int j=0;j<page[i].size();j++)
		{
			out_stream<< page[i][j]<< " "; 
		}
		out_stream<<endl;
	}
	return;
}

void page_sorter(long long int & total_rows,long long int & page_size,long long int & total_columns,vector< long long int > & preference,vector < long long int > & reverse_order,string inputfile,string outputfile)
{
	vector< string > data,tempdata;
	vector< vector < string > > page;
	string line;
	stringstream linestream;
	ifstream infile(inputfile);
	string token;
	ofstream out_stream;
	out_stream.open(outputfile);
	long long int counter=0;
	while(getline(infile,line))
	{
		stringstream linestream(line);
		while(linestream>>token)
			tempdata.push_back(token);
		for(long long int i=0;i<tempdata.size();i++)
			data.push_back(tempdata[preference[i]]);
		tempdata.clear();
		page.push_back(data);
		data.clear();
		counter++;
		total_rows++;
		if(counter==page_size)
		{
			print_page(page,reverse_order,out_stream);
			counter=0;
			page.clear();
		}
	}
	print_page(page,reverse_order,out_stream);
	infile.close();
	out_stream.close();
	return;
}

