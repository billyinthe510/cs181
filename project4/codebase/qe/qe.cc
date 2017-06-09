
#include "qe.h"
#include <cstring>
#include <string>
#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cmath>
using namespace std;
Filter::Filter(Iterator* input, const Condition &condition) 
{
	in = input;
	con = condition;
}

RC Filter::getNextTuple(void *data)
{
	RC rc;
	bool found = false;
	vector<Attribute> attrs;
	getAttributes(attrs);
	//cout <<"ATTRIBUTE SIZE: " << attrs.size() << endl;
	while(found == false && (rc = in->getNextTuple(data)) != QE_EOF)
	{
		int offset = ceil((double)attrs.size() / 8);
		for(unsigned i = 0; i < attrs.size(); i++)
		{
			
			switch(attrs[i].type)
			{
				case TypeInt:
					int val;
					memcpy(&val, (char*)data + offset, sizeof(int));
					offset += sizeof(int);
					
					if(attrs[i].name == con.lhsAttr)
					{
						//check value
						//cout <<"val: " << val << endl;
						if(con.bRhsIsAttr == true)
						{
							
						}
						else
						{
							int conVal;
							memcpy(&conVal, con.rhsValue.data, sizeof(int));
							//cout << "Looking: " << conVal << endl;
								switch(con.op)
								{
									case EQ_OP:
										if(val == conVal)
											found = true;
										break;
									case LT_OP:
										if(val < conVal)
											found = true;
										break;
									case LE_OP:
										//cout << "IS THIS TRUE: " << endl;
										if(val <= conVal)
										{
											//cout <<"yes it was" << endl;
											found = true;
										}	
										break;	
									case GT_OP:
										if(val > conVal)
											found = true;
										break;
									case GE_OP:
										if(val >= conVal)
											found = true;
										break;
									case NE_OP:
										if(val != conVal)
											found = true;
										break;
									case NO_OP:
										found = true;
										break;
								}
						}
					}
					break;
				case TypeReal:
					float value;
					memcpy(&value, (char*)data + offset, sizeof(int));
					offset += sizeof(int);
					if(attrs[i].name == con.lhsAttr)
					{
						//check value
						//cout <<"value: " << value << endl;
						if(con.bRhsIsAttr == true)
						{
							
						}
						else
						{
							float conVal;
							memcpy(&conVal, con.rhsValue.data, sizeof(int));
							//cout << "Looking for: " << conVal << endl;
								switch(con.op)
								{
									case EQ_OP:
										if(value == conVal)
											found = true;
										break;
									case LT_OP:
										if(value < conVal)
											found = true;
										break;
									case LE_OP:
										if(value <= conVal)
											found = true;
										break;
									case GT_OP:
										if(value > conVal)
											found = true;
										break;
									case GE_OP:
										if(value >= conVal)
											found = true;
										break;
									case NE_OP:
										if(value != conVal)
											found = true;
										break;
									case NO_OP:
										found = true;
										break;
								}
						}
					}
					break;
				case TypeVarChar:
				{
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));
					offset += sizeof(int);
					char* str = (char*) malloc(len + 1);
					memcpy(str, (char*) data + offset, len);
					offset += len;
					str[len] = '\0';
					if(attrs[i].name == con.lhsAttr)
					{
						//check value
						//cout << "str: " << str << endl;
						if(con.bRhsIsAttr == true)
						{
							
						}
						else
						{
							int conLen;
							memcpy(&conLen, con.rhsValue.data, sizeof(int));
							char* conStr = (char*) malloc(conLen + 1);
							memcpy(conStr, (char*) con.rhsValue.data + sizeof(int), conLen);
							conStr[conLen] = '\0';
							switch(con.op)
							{
								case EQ_OP:
									if(strcmp(str, conStr) == 0)
										found = true;
									break;
								case LT_OP:
									if(strcmp(str, conStr) < 0)
										found = true;
									break;
								case LE_OP:
									if(strcmp(str, conStr) <= 0)
										found = true;
									break;
								case GT_OP:
									if(strcmp(str, conStr) > 0)
										found = true;
									break;
								case GE_OP:
									if(strcmp(str, conStr) >= 0)
										found = true;
									break;
								case NE_OP:
									if(strcmp(str, conStr) != 0)
										found = true;
									break;
								case NO_OP:
									found = true;
									break;
							}
						}
					}
					free(str);
					break;
				}
			}
			if(found == true)
			{
				i = attrs.size();
				//cout <<"HELLO LETS GET OUT OF HERE" << endl;
			}
		}
		
	}
	//cout << "RC: " << rc << endl;
	if(rc)
		return rc;
	return SUCCESS;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
	in->getAttributes(attrs);
}

Project::Project(Iterator* input, const vector<string> &attrNames)
{
	in = input;
	names = attrNames;
}

RC Project::getNextTuple(void* data)
{
	RC rc;
	bool found = false;
	vector<Attribute> attrs;
	in->getAttributes(attrs);
	//cout <<"ATTRIBUTE SIZE: " << attrs.size() << endl << "PROJECT START" << endl;
	void *readData = malloc(200); //equals bufSize
	while(found == false && (rc = in->getNextTuple(readData)) != QE_EOF)
	{
		int offset = ceil((double)attrs.size() / 8);
		int tup_offset = 0;
		memcpy((char*)data + tup_offset, readData, ceil((double)attrs.size() / 8));
		tup_offset += ceil((double)attrs.size() / 8);
		for(unsigned i = 0; i < attrs.size(); i++)
		{
			//cout << "i:" << endl;
			switch(attrs[i].type)
			{
				case TypeInt:
					int val;
					memcpy(&val, (char*)readData + offset, sizeof(int));
					offset += sizeof(int);
					//cout << "val:" << val << endl << tup_offset << endl;
					for(unsigned j = 0; j < names.size(); j++)
					{
						if(attrs[i].name == names[j])
						{
							memcpy((char*)data + tup_offset, &val, sizeof(int));
							tup_offset += sizeof(int);
							found = true;
						}
					}
					break;
				case TypeReal:
					float value;
					memcpy(&value, (char*)readData + offset, sizeof(float));
					offset += sizeof(float);
					//cout << "value:" << value << endl;
					for(unsigned j = 0; j < names.size(); j++)
					{
						if(attrs[i].name == names[j])
						{
							memcpy((char*)data + tup_offset, &value, sizeof(float));
							tup_offset += sizeof(float);
							found = true;
						}
					}
					break;
				case TypeVarChar:
				{
					int len;
					memcpy(&len, (char*) readData + offset, sizeof(int));
					offset += sizeof(int);
					char* str = (char*) malloc(len + 1);
					memcpy(str, (char*) readData + offset, len);
					offset += len;
					str[len] = '\0';
					for(unsigned j = 0; j < names.size(); j++)
					{
						if(attrs[i].name == names[j])
						{
							memcpy((char*)data + tup_offset, &len, sizeof(int));
							tup_offset += sizeof(int);
							memcpy((char*)data + tup_offset, str, len);
							tup_offset += len;
							found = true;
						}
					}
					free(str);
					break;
				}
			}
			
		}
	}
	//cout << "RC: " << rc << endl;
	free(readData);
	if(rc)
		return rc;
	return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	vector<Attribute> temp;
	in->getAttributes(temp);
	for(unsigned i = 0; i < names.size(); i++)
	{
		for(unsigned j = 0; j < temp.size(); j++)
		{
			if(names[i] == temp[j].name)
				attrs.push_back(temp[j]);
		}		
	}
		
}

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
		IndexScan *rightIn,          // IndexScan Iterator of input S
		const Condition &condition)   // Join condition   
{
	in = leftIn;
	idxIn = rightIn;
	con = condition;
}

RC INLJoin::getNextTuple(void *data)
{
	RC rc;
	bool found = false;
	vector<Attribute> attrs;
	getAttributes(attrs);
	vector<Attribute> idxAttrs;
	idxIn->getAttributes(idxAttrs);
	//cout <<"ATTRIBUTE SIZE: " << attrs.size() << endl;
	void *readData = malloc(200); //equals bufSize
	void *innerData = malloc(200);
	//cout <<"INL START" << endl;
	while(found == false && (rc = in->getNextTuple(readData)) != QE_EOF)
	{
		int offset = ceil((double)attrs.size() / 8);
		void* outVal = malloc(sizeof(int));
		void* inVal = malloc(sizeof(int));
		char* outStr;
		char* inStr;
		int attrNum = 0;
		int idxAttrNum = 0;
		for(unsigned i = 0; i < attrs.size(); i++)
		{
			switch(attrs[i].type)
			{
				case TypeInt:
					
					if(attrs[i].name == con.lhsAttr)
					{
						memcpy(outVal, (char*) readData + offset, sizeof(int));
						attrNum = i;
					}
					offset += sizeof(int);
					break;
				case TypeReal:
					
					if(attrs[i].name == con.lhsAttr)
					{
						memcpy(outVal, (char*) readData + offset, sizeof(float));
						attrNum = i;
					}
					offset+= sizeof(float);
					break;
				case TypeVarChar:
					int len;
					memcpy(&len, (char*) readData + offset, sizeof(int));
					offset += sizeof(int);
					if(attrs[i].name == con.lhsAttr)
					{
						outStr = (char*)malloc(len + 1);
						memcpy(outStr, (char*) readData + offset, len);
						attrNum = i;
						outStr[len] = '\0';
					}
					break;
			}
		}
		while(found == false && (rc = idxIn->getNextTuple(innerData)) != QE_EOF)
		{
			char nullBits;
			char outBits;
			char inBits;
			int outBitsSize = ceil((double)attrs.size() / 8);
			int inBitsSize = ceil((double)attrs.size() / 8);
			int inner_offset = ceil((double)idxAttrs.size() / 8);
			memcpy(&outBits, readData, ceil((double)attrs.size() / 8));
			memcpy(&inBits, innerData, ceil((double)idxAttrs.size() / 8));
			nullBits = outBits + inBits;
			int nullSize = ceil(((double)attrs.size() + (double)idxAttrs.size()) / 8);
			memcpy(data, &nullBits, nullSize);
			for(unsigned i = 0; i < idxAttrs.size(); i++)
			{
				if(con.bRhsIsAttr == true)
				{
					switch(attrs[i].type)
					{
						case TypeInt:
							if(idxAttrs[i].name == con.rhsAttr)
							{
								idxAttrNum = i;
								memcpy(inVal, (char*) innerData + inner_offset, sizeof(int));
							}
							inner_offset += sizeof(int);
						
							break;
						case TypeReal:
							if(idxAttrs[i].name == con.rhsAttr)
							{
								memcpy(inVal, (char*) innerData + inner_offset, sizeof(float));
								idxAttrNum = i;
							}
							inner_offset += sizeof(float);
							
							break;
						case TypeVarChar:
							int len;
							memcpy(&len, (char*) innerData + inner_offset, sizeof(int));
							inner_offset += sizeof(int);
							if(idxAttrs[i].name == con.rhsAttr)
							{
								inStr = (char*)malloc(len + 1);
								memcpy(inStr, (char*) innerData + inner_offset, len);
								idxAttrNum = i;
								inStr[len]= '\0';
							}
							inner_offset += len;
							
							break;
					}
				}	
			}
			if(attrs[attrNum].type == idxAttrs[idxAttrNum].type)
			{
				
				if(attrs[attrNum].type == TypeInt)
				{
					int val;
					int conVal;
					memcpy(&val, outVal, sizeof(int));
					memcpy(&conVal, inVal, sizeof(int));
					
					switch(con.op)
					{
						case EQ_OP:
							if(val == conVal)
								found = true;
							break;
						case LT_OP:
							if(val < conVal)
								found = true;
							break;
						case LE_OP:
							if(val <= conVal)
							{
								found = true;
							}	
							break;	
						case GT_OP:
							if(val > conVal)
								found = true;
							break;
						case GE_OP:
							if(val >= conVal)
								found = true;
							break;
						case NE_OP:
							if(val <= conVal)
								found = true;
							break;
						case NO_OP:
							found = true;
							break;
					}
				}
				else if(attrs[attrNum].type == TypeReal)
				{
					float val;
					float conVal;
					memcpy(&val, outVal, sizeof(float));
					//cout << val << endl;
					memcpy(&conVal, inVal, sizeof(float));
					//cout << conVal << endl;
					switch(con.op)
					{
						case EQ_OP:
							if(val == conVal)
								found = true;
							break;
						case LT_OP:
							if(val < conVal)
								found = true;
							break;
						case LE_OP:
							if(val <= conVal)
							{
								found = true;
							}	
							break;	
						case GT_OP:
							if(val > conVal)
								found = true;
							break;
						case GE_OP:
							if(val >= conVal)
								found = true;
							break;
						case NE_OP:
							if(val <= conVal)
								found = true;
							break;
						case NO_OP:
							found = true;
							break;
					}
				}
				else if(attrs[attrNum].type == TypeVarChar)
				{
					switch(con.op)
					{
						case EQ_OP:
							if(strcmp(inStr, outStr) == 0)
								found = true;
							break;
						case LT_OP:
							if(strcmp(inStr, outStr) < 0)
								found = true;
							break;
						case LE_OP:
							if(strcmp(inStr, outStr) <= 0)
								found = true;
							break;
						case GT_OP:
							if(strcmp(inStr, outStr) > 0)
								found = true;
							break;
						case GE_OP:
							if(strcmp(inStr, outStr) >= 0)
								found = true;
							break;
						case NE_OP:
							if(strcmp(inStr, outStr) != 0)
								found = true;
							break;
						case NO_OP:
							found = true;
							break;
					}
					free(inStr);
					free(outStr);
				}
			}
			if(found == true)
			{
				memcpy((char*)data + nullSize, (char*)readData + outBitsSize, offset - outBitsSize);
				memcpy((char*)data + nullSize + offset - outBitsSize, (char*)innerData + inBitsSize, inner_offset - inBitsSize);
			}
			
		}
		idxIn->setIterator(NULL, NULL, true, true);
		free(inVal);
		free(outVal);
	}
	free(readData);
	free(innerData);
	//cout << "RC: " << rc << endl;
	if(rc)
		return rc;
	return SUCCESS;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	in->getAttributes(attrs);
}

// ... the rest of your implementations go here
