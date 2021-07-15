
#include <bits/stdc++.h>
using namespace std;


typedef map<string, map<string, vector<int> > > database;
typedef map<string, map<int, string> > db_info;

bool error;

struct my_db{
    database data;
    db_info meta;
};

struct parsed_query{
    vector < string > columns;
    vector < string > tables;
    vector < string > comparisons;
    vector < string > operators;
    vector < string > agg_functions;
};

struct seg_data{
    int n;          
    database data;  
    map < string , int > m;     
    vector < string > table;    
    vector < int > size;
    vector < vector < string > > column;
    vector < int > pointer;
    vector < int > cartesian;
};

struct key_data{
    vector < string > sg;
    vector < string > tables;
    vector < string > meta;
    vector < string > dis;
    vector < string > fnc;
};


vector< vector < int > > result_table;
set< string > redundant;
string sort_c;
int o_flag=0;
int c_index=1;
int w_flag=0;
int sf=0;

void DISTINCT(struct seg_data* seg, struct parsed_query* query);
bool IS_Number(string s);
bool sortcol_asc( const vector<int>& v1, const vector<int>& v2 ) ;
bool sortcol_desc( const vector<int>& v1, const vector<int>& v2 );
void SELECT(struct seg_data * seg,struct parsed_query * query, struct my_db * db);
bool WHERE(struct seg_data *seg,string op,struct parsed_query * query);
bool tests_WHERE(struct seg_data* seg, struct parsed_query* query);
void tests_tuples(struct seg_data * seg,struct my_db * db, struct parsed_query* query);
void segregate(struct seg_data * seg,struct my_db * db, struct parsed_query* query);
struct seg_data * populate_seg(struct parsed_query * query , struct my_db * db);
bool agg_func_cal(struct seg_data * seg,string func,struct my_db * db,struct parsed_query * query);
struct parsed_query* cols_to_table(struct my_db* db, struct parsed_query* query, string cur_column, int index_string, bool flag);
struct parsed_query* error_handling(struct parsed_query* query, struct my_db* db);
struct parsed_query* handle_star(struct parsed_query* query,my_db* db);
struct parsed_query* fill_agg_functions(struct parsed_query* query);
struct parsed_query * parseit(string input);
struct my_db* retrieve_meta();
struct my_db* fill_data(my_db* db);


int main(int argc, char* argv[])
{
    my_db* db = retrieve_meta();
    fill_data(db);
    string q="";
    string qr=argv[1];

    if(qr.back() != ';'){
    	cout << "ERROR\n";
    	return 0;

    }
    int ll=qr.length();
    qr=qr.substr(0,ll-1);

    struct parsed_query * query=parseit(qr);
    query = fill_agg_functions(query);
    query = handle_star(query,db);
    query = error_handling(query,db);
    if(error)
        cout << "ERROR\n";
    else
    {
        
        struct seg_data * seg= populate_seg(query,db);
        segregate(seg,db, query);
        if(agg_func_cal(seg,query->agg_functions[0], db, query))
            return 0;
        DISTINCT(seg, query);
        SELECT(seg,query,db);

    }
    return 0;
}


void DISTINCT(struct seg_data* seg, struct parsed_query* query)
{
    int i;
    bool flag  = false;
    for(i=0;i<query->agg_functions.size();i++)
        if(query->agg_functions[i]=="distinct")
        {
            flag=true;
            break;
        }
    if(!flag)
        return;
    string col_name = query->columns[i];
    set<int> dis_col;
    int col_index = seg->m[col_name];
    for(i=0;i<result_table.size();i++)
    {
        if(dis_col.find(result_table[i][col_index]) == dis_col.end())
            dis_col.insert(result_table[i][col_index]);
        else
        {
            result_table.erase(result_table.begin()+i);
            i--;
        }
    }
}

bool IS_Number(string s)
{
    if(s[0]=='+' || s[0]=='-' || isdigit(s[0]))
    {
        for(int i=1;i<s.size();i++)
            if(!isdigit(s[i]))
                return false;
    }
    else
        return false;
    return true;
}
bool sortcol_asc( const vector<int>& v1, const vector<int>& v2 ) { 
 return v1[c_index] < v2[c_index]; 
} 

bool sortcol_desc( const vector<int>& v1, const vector<int>& v2 ) { 
 return v1[c_index] > v2[c_index]; 
} 

void SELECT(struct seg_data * seg,struct parsed_query * query, struct my_db * db)
{
    int flag = 0;
    //COlumn headings
    for(int i=0;i<query->columns.size();i++)
    {
        if(i!=0 && flag==1)
        {
            flag = 0;
            cout << ",";
        }
        if(redundant.find(query->columns[i])==redundant.end())
        {
            flag = 1;
           
            cout << query->columns[i];
         
        }
    }
    cout << endl;
    if(o_flag){

      for(int i=0;i<query->tables.size();i++)
        {
            if(db->data[query->tables[i]].find(sort_c) != db->data[query->tables[i]].end())
            {
               
                    sort_c=query->tables[i] + "." + sort_c;
                    
                }
              }
      c_index=seg->m[sort_c];
      
      if(sf==0)
      sort(result_table.begin(), result_table.end(),sortcol_asc);
      else
      sort(result_table.begin(), result_table.end(),sortcol_desc);

    }
    //Printing data
    for(int i=0;i<result_table.size();i++)
    {
        for(int j=0;j<query->columns.size();j++)
        {
            if(j!=0 && flag==1)
            {
                flag = 0;
                cout << ",";
            }
            if(redundant.find(query->columns[j]) != redundant.end())
            {
               
            }
            else{
            	 cout << result_table[i][seg->m[query->columns[j]]];
                flag = 1;
            }
        }
        cout << endl;
    }

}

bool WHERE(struct seg_data *seg,string op,struct parsed_query * query)
{
    vector <int> candidate(seg->cartesian);
    string token,comp;
    stringstream ss;
    ss << op;
    vector<string> tests;
    while(ss>>token)
        tests.push_back(token);
    comp = tests[1];
    if(comp=="=")
    {
        if(IS_Number(tests[0]))
        {
            if(atoi(tests[0].c_str())==candidate[seg->m[tests[2]]])
                return true;
        }
        else if(IS_Number(tests[2]))
        {
            if(candidate[seg->m[tests[0]]]==atoi(tests[2].c_str()))
                return true;
        }
        else if(candidate[seg->m[tests[0]]]==candidate[seg->m[tests[2]]])
        {
            redundant.insert(tests[2]);
            return true;
        }
    }
    else if(comp==">")
    {
        if(IS_Number(tests[0]))
        {
            if(atoi(tests[0].c_str()) > candidate[seg->m[tests[2]]])
                return true;
        }
        else if(IS_Number(tests[2]))
        {
            if(candidate[seg->m[tests[0]]] > atoi(tests[2].c_str()))
                return true;
        }
        else if(candidate[seg->m[tests[0]]] > candidate[seg->m[tests[2]]])
        {
          
            return true;
        }
    }
    if(comp==">=")
    {
        if(IS_Number(tests[0]))
        {
            if(atoi(tests[0].c_str()) >= candidate[seg->m[tests[2]]])
                return true;
        }
        else if(IS_Number(tests[2]))
        {
            if(candidate[seg->m[tests[0]]] >= atoi(tests[2].c_str()))
                return true;
        }
        else if(candidate[seg->m[tests[0]]] >= candidate[seg->m[tests[2]]])
        {
           
          // redundant.insert(tests[2]);
            return true;
        }
    }
    if(comp=="<")
    {
        if(IS_Number(tests[0]))
        {
            if(atoi(tests[0].c_str()) < candidate[seg->m[tests[2]]])
                return true;
        }
        else if(IS_Number(tests[2]))
        {
            if(candidate[seg->m[tests[0]]] < atoi(tests[2].c_str()))
                return true;
        }
        else if(candidate[seg->m[tests[0]]] < candidate[seg->m[tests[2]]])
        {
            
            return true;
        }
    }
    if(comp=="<=")
    {
        if(IS_Number(tests[0]))
        {
            if(atoi(tests[0].c_str()) <= candidate[seg->m[tests[2]]])
                return true;
        }
        else if(IS_Number(tests[2]))
        {
            if(candidate[seg->m[tests[0]]] <= atoi(tests[2].c_str()))
                return true;
        }
        else if(candidate[seg->m[tests[0]]] <= candidate[seg->m[tests[2]]])
        {
            return true;
        }
    }
    
    return false;
}


bool tests_WHERE(struct seg_data* seg, struct parsed_query* query)
{
    if(query->comparisons.size()==0)
        return true;
    bool cnd = true;
    vector<bool> cnds;
    for(int i=0;i<query->comparisons.size();i++)
        cnds.push_back(WHERE(seg, query->comparisons[i], query));
    if(query->operators.size()==0)
        return cnds[0];
    cnd = cnds[0];
    for(int i=0;i<query->operators.size();i++)
        if(query->operators[i]=="and")
            cnd = cnd & cnds[i+1];
        else if(query->operators[i]=="or")
            cnd = cnd | cnds[i+1];
    return cnd;
}


void tests_tuples(struct seg_data * seg,struct my_db * db, struct parsed_query* query)
{
    vector <int> p;
    p = seg->pointer;
    for(int i=0;i<p.size();i++)
    {
        string table_name = seg->table[i];
        for(int j=0;j<seg->column[i].size();j++)
        {
            string tmp=table_name+"."+seg->column[i][j];
            seg->cartesian[seg->m[tmp]]=db->data[table_name][seg->column[i][j]][seg->pointer[i]];
        }
    }
    if(tests_WHERE(seg,query))
    {
        result_table.push_back(seg->cartesian);
    }
    
}



void segregate(struct seg_data * seg,struct my_db * db, struct parsed_query* query)
{
    int last = seg->n - 1;
    int first = 0;
    while(1)
    {
        while(seg->pointer[first]>=seg->size[first])
    {
        if(last==first)
            break;
        seg->pointer[first+1]+=1;
        seg->pointer[first] = 0;
        first++;
    }
    if(seg->pointer[last] < seg->size[last])
        tests_tuples(seg, db, query);
      seg->pointer[0]+=1;
      if(seg->pointer[last]>=seg->size[last])
            break;
    }
}



struct seg_data * populate_seg(struct parsed_query * query , struct my_db * db)
{
    int count = 0;
    map <string, int > maps;
    vector<string> tables;
    vector <int> sizes;
    vector <vector <string> > columns;
    for(int j=0;j<query->tables.size();j++)
    {
        string tmp = query->tables[j];
        vector <string> tmpv;
        map < string , vector < int > > ::iterator it;
        string y = "";
        for(it = db->data[tmp].begin();it!=db->data[tmp].end();it++)
        {
            string x = tmp + "." + it->first;
            tmpv.push_back(it->first);
            maps[x] = count;
            count++;
            y = it->first;
        }
        tables.push_back(tmp);
        columns.push_back(tmpv);
        int l = db->data[tmp][y].size();
        sizes.push_back(l);
    }
    vector < int > pointers;
    vector < int > potential;

    for(int i=0;i<count;i++)
    {
        potential.push_back(0);
    }
    int n_tables=sizes.size();
    for(int i=0;i<n_tables;i++)
    {
        pointers.push_back(0);
    }
    seg_data * my_data = new seg_data;

    my_data->table=tables;
    my_data->n=n_tables;
    my_data->data=db->data;
    my_data->pointer=pointers;
    my_data->m=maps;
    my_data->column=columns;
    my_data->size=sizes;
    my_data->cartesian=potential;
    
    return my_data;
}

bool agg_func_cal(struct seg_data * seg,string func,struct my_db * db,struct parsed_query * query)
{
	string col = query->columns[0];

        size_t pos = col.find(".");
        string table_name = query->tables[0];
        vector<int> final ;

        for(int i=0;i<result_table.size();i++)
    {
        
            	 final.push_back(result_table[i][seg->m[col]]);
              
            
        }
      
    if (func== "count")
    {
    	cout << result_table.size() <<"\n";
    	return true;  
    }

    if(func == "max" || func == "min")
    {
        
        int _max = INT_MIN, _min = INT_MAX;
        
        for(int i=0;i<final.size();i++)
        {
            _max = max(_max, final[i]);
            _min = min(_min, final[i]);
        }
        
        cout << func << "(" << query->columns[0] << ")" << endl;
        if(func=="max")
            cout << _max << endl;
        else if(func=="min")
            cout << _min << endl;
        
        return true;
    }

    if(func == "sum" || func == "avg")
    {
    	
        long long int _sum = 0;
        double _avg = 0.0;
        for(int i=0;i<final.size();i++)
        _avg += final[i];
       
        _sum = _avg;
        _avg = 1.0*_avg/(1.0*final.size());
        cout << func << "(" << query->columns[0] << ")" << endl;
        
        if(func=="avg")
        {
            cout.setf(ios::fixed);
            cout<<setprecision(4)<<_avg<<endl;
        }
        else if(func=="sum")
            cout << _sum << endl;
        return true;
    }
    return false;
}

struct parsed_query* cols_to_table(struct my_db* db, struct parsed_query* query, string cur_column, int index_string, bool flag)
{
    size_t d_index;
    d_index = cur_column.find('.');

    if(d_index!=string::npos)
    {
        if((db->data.find(cur_column.substr(0,d_index))==db->data.end() )|| (db->data[cur_column.substr(0,d_index)].find(cur_column.substr(d_index+1)) == db->data[cur_column.substr(0,d_index)].end()))
            error = true;
       
    }
    else
    {
        int count = 0;
        string new_clmn="", cmp = "";
        for(int i=0;i<query->tables.size();i++)
        {
            if(db->data[query->tables[i]].find(cur_column) != db->data[query->tables[i]].end())
            {

                count++;
                if(flag)
                {
                    new_clmn=query->tables[i] + "." + cur_column;
                    query->columns[index_string] = new_clmn;
                }
                else
                {
                    new_clmn=query->tables[i] + "." + cur_column;
                    cmp = query->comparisons[index_string];
                    query->comparisons[index_string] = "";
                    stringstream comp;
                    comp << cmp;
                    string token;
                    while(comp >> token)
                    {
                        if(token.compare(cur_column)==0)
                            query->comparisons[index_string]+=new_clmn+" ";
                        else
                            query->comparisons[index_string]+=token+" ";
                    }
                }
            }
        }
        if(count!=1)
            error = true;
        else
        { }
    }
    return query;
}

struct parsed_query* error_handling(struct parsed_query* query, struct my_db* db)
{
    vector<string>::iterator it1;
    
    for(it1 = (query->tables).begin();it1!=(query->tables).end();it1++)
    {
        if((db->meta).find(*it1) == (db->meta).end())
            error = true;
    }

    for(int i=0;i<(query->columns).size();i++)
    {
        string cur_column = query->columns[i];
        query = cols_to_table(db, query, cur_column, i, true);
    }

    for(int i=0;i<(query->comparisons).size();i++)
    {
        string cur_column;
        cur_column = query->comparisons[i];
        stringstream comp;
        string token;
        comp << cur_column;
        int count = 0;
        while(comp>>token)
        {
            if(count==0 || count==2)
            {
                if(!IS_Number(token))
                    query = cols_to_table(db, query, token, i, false);
                else
                {}
            }
            count++;
            token="";
        }
    }
    return query;

}


struct parsed_query* handle_star(struct parsed_query* query,my_db* db)
{
if(query->columns.size()==1 && query->columns[0].compare("*")==0)
    {
        query->columns.clear();
        query->agg_functions.clear();
        string new_clmn="";
        map< string, vector< int > >::iterator colit;
        for(int i=0;i<((query->tables).size());i++)
        {
            for(colit=db->data[query->tables[i]].begin();colit!=db->data[query->tables[i]].end();colit++)
            {
                new_clmn="";
                new_clmn+=query->tables[i]+"."+colit->first;
                query->columns.push_back(new_clmn);
                query->agg_functions.push_back("NULL");
            }
        }
    }
    return query;
}

struct parsed_query* fill_agg_functions(struct parsed_query* query)
{
    for(int i=0;i<query->columns.size();i++)
    {
        string col = query->columns[i];
        size_t pos;
        if(col.find("distinct")!=string::npos)
        {
            pos = col.find("distinct");
            query->agg_functions.push_back("distinct");
            int l = col.size();
            query->columns[i] =  col.substr(pos+9, l-10);
            continue;
        }
        else if(col.find("avg")!=string::npos)
        {
            pos = col.find("avg");
            query->agg_functions.push_back("avg");
            int l = col.size();
            query->columns[i] = col.substr(pos+4, l-5);
            continue;
        }
         else if(col.find("min")!=string::npos)
        {
            pos = col.find("min");
            query->agg_functions.push_back("min");
            int l = col.size();
            query->columns[i] = col.substr(pos+4, l-5);
            continue;
        }
        else if(col.find("max")!=string::npos)
        {
            pos = col.find("max");
            query->agg_functions.push_back("max");
            int l = col.size();
            query->columns[i] = col.substr(pos+4, l-5);
            continue;
        }
        else if(col.find("sum")!=string::npos)
        {
            pos = col.find("sum");
            query->agg_functions.push_back("sum");
            int l = col.size();
            query->columns[i] = col.substr(pos+4, l-5);
            continue;
        }
        
        else if(col.find("count")!=string::npos)
        {
            pos = col.find("count");
            query->agg_functions.push_back("count");
            int l = col.size();
            query->columns[i] = col.substr(pos+6, l-7);
            continue;
        }
        
       
        else
            query->agg_functions.push_back("NULL");
      //  cout << query->agg_functions[i] << endl;
    }
    return query;
}

struct parsed_query * parseit(string input)
{
  struct parsed_query * myquery=new parsed_query;
  int query_len=input.size();
  replace (input.begin(), input.end(), ',' , ' ');
  stringstream input_stream;
  input_stream<<input;
  string token;
  string condn="";
  string expression="";
  int identifier=-1;
  while(input_stream>>token)
  {
  	if(token.compare(";")==0)
  	continue;

  	if(token.compare("DESC")==0){
  		sf=1;
  		continue;
  	}


  	if(token.compare("ASC")==0){
  		sf=0;
  		continue;
  	}

    if(token.compare("SELECT")==0 || token.compare("select")==0 || token.compare("FROM")==0 || token.compare("from")==0  || token.compare("order")==0 || token.compare("by")==0)
    {
      identifier++;
      continue;
    }
    if( token.compare("WHERE")==0 || token.compare("where")==0){
      identifier=identifier+1;
      w_flag=1;
      continue;
    }
    switch(identifier)
    {
      case 0:
        {
          (myquery->columns).push_back(token);
          break;
        }
      case 1:
        {
        	if(token.back()==';')
            {
            int ll=token.length();
            token=token.substr(0,ll-1);
        	}
          (myquery->tables).push_back(token);
          break;
        }
      case 2:
        {
          if(token=="AND")
            token="and";
          else if(token=="OR")
            token="or";
          if(token=="and" or  token=="or")
          {
            (myquery->operators).push_back(token);
            if(condn.compare("")!=0)
            {
              string s1="",s2="",s3="";
              int tests=0;
              for(int i=0;i<condn.size();i++)
              {
                if(condn[i]=='<' or condn[i]=='>' or condn[i]=='=' or condn[i]=='!')
                  tests=1; 
                else if(tests==1 and condn[i]!='=')
                  tests=2; 
                if(tests==0 and condn[i]!=' ')
                  s1+=condn[i]; 
                if(tests==1 and condn[i]!=' ')
                  s2+=condn[i];
                if(tests==2 and condn[i]!=' ')
                  s3+=condn[i];
                expression=s1+" "+s2+" "+s3;
              }
              (myquery->comparisons).push_back(expression);
            }
            condn="";
            expression="";
            continue;
          }
          condn+=token;
          condn+=" ";
          break;
        }
        case 3:
        {
          if(w_flag==0){
            sort_c = token;
            if(sort_c.back()==';')
            {
            int ll=token.length();
            sort_c=token.substr(0,ll-1);
        	}
          	o_flag=1;
          }
          break;
        }
        case 4:
        {
          sort_c = token;
          if(sort_c.back()==';')
            {
            int ll=token.length();
            sort_c=token.substr(0,ll-1);
        	}
          o_flag=1;
          break;

        }
      default:
        break;
    }
  }
  if(condn!="")
  {
  	if(condn.back()==';')
            {
            int ll=condn.length();
            condn=condn.substr(0,ll-1);
        	}
    string s1="",s2="",s3="";
    int tests=0;
    for(int i=0;i<condn.size();i++)
    {
      if(condn[i]=='<' or condn[i]=='>' or condn[i]=='=' or condn[i]=='!')
        tests=1; 
      else if(tests==1 and  condn[i]!='=')
        tests=2; 
      if(tests==0 and condn[i]!=' ')
        s1+=condn[i]; 
      if(tests==1 and condn[i]!=' ')
        s2+=condn[i];
      if(tests==2 and condn[i]!=' ')
        s3+=condn[i];
      expression=s1+" "+s2+" "+s3;
    }
    (myquery->comparisons).push_back(expression);
  }
  return myquery;
}

struct my_db* retrieve_meta()
{
    ifstream meta("metadata.txt");

    string token, table_name;
    database data;
    db_info tmpm;
    int col_number;
    int flag=1;
    while(meta>>token) 
    {
        if(flag==0)
        {
            if(token[0]=='<')
                flag = 1;
            else
            {
                vector<int> temp_col;
                data[table_name][token] = temp_col;
                tmpm[table_name][++col_number] = token;
            }
        }
        else
        {
            if(token[0]=='<')
                flag = 0;
            meta >> table_name;
            map <string, vector<int> > col;
            data[table_name] = col;
            col_number = 0;
        }
    }
    meta.close();
    my_db* new_db = new my_db;
    new_db->data = data;
    new_db->meta = tmpm;
    return new_db;
}

struct my_db* fill_data(my_db* db)
{
    map<string, map<string, vector<int> > >::iterator it1;
    map<string, vector<int> > clm;
   
    for(it1 = db->data.begin();it1!=db->data.end();it1++)
    {
        string filename = it1->first;
        filename.append(".csv");
        ifstream csv_file(filename.c_str());
        string token;
        while(csv_file>>token)
        {
            for(int l = 0;l<token.size();l++)
                if(token[l] == ',')
                    token[l] = ' ';
            stringstream ss;
            ss << token;
            string temp;
            int col_number = 1;
            while(ss>>temp)
            {
                string col_name = db->meta[it1->first][col_number];
                int val = stoi(temp);
                db->data[it1->first][col_name].push_back(val);
                col_number=col_number+1;
            }
        }
        csv_file.close();
    }
    return db;

}


