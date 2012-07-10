import time
import datetime
import re
import mechanize
import cookielib
import json
import BeautifulSoup

class PageCrawler(object):
    
    def __init__(self, base_url, login_str, login, pwd, sleep_time = 2):
        '''loanIDs is a list of loanIDs to be grabbed'''
        self.login = login
        self.password = pwd
        self.base_url = base_url
        self.login_str = login_str
        self.sleep_time = sleep_time
        self.html = {}     
        self.br = self.setup_browser()
        
    def setup_browser(self):
        br = mechanize.Browser()
        cj = cookielib.LWPCookieJar()
        br.set_cookiejar(cj)
        br.set_handle_robots(False)
        
        return br
        
    def crawl(self, page_params):
        '''go through the list of page parameters and get the html and store'''
        for p in page_params:
            html = self.get_html(p)
            
            html = self.auth_check(html, p)
                
            self.html[p] = html
            print 'Got page with parameters %s' % str(p)
            time.sleep(self.sleep_time)
    
    def sign_in(self):
        '''Sign into LendingClub'''
        try:
            self.br.open('https://www.lendingclub.com/account/gotoLogin.action')
            self.br.select_form(nr=0)
            self.br.form['login_email'] = self.login
            self.br.form['login_password'] = self.password
            self.br.submit()
            print 'logged in as %s' % self.login
        except:
            raise Exception('failed to login')
    
    def auth_check(self, html, param):
        '''Check to see if browser is logged in, if not log in'''
        if self.login_str in html:
            self.sign_in()
            html = self.get_html(param)
            if self.login_str in html:
                raise Exception('Unable to login')         
        return html
    
    def get_html(self, param):
        self.br.open(self.base_url % param)
        return self.br.response().read()
    
    def get_data(self):
        '''returns a dictionary param:html'''
        return self.html


class NoteOrders(PageCrawler):
    '''Gets the most recent list of traded notes from LendingClub's foliofn platform'''
    
    def __init__(self, login, pwd):
        self.login = login
        self.password = pwd
        self.br = self.setup_browser()
        self.sign_in()
            
    def grab_data(self, start_index, page_size):
        '''Pull the data from the LC website'''
        try:
            self.br.open('https://www.lendingclub.com/foliofn/tradingInventory.action')
            self.br.open('https://www.lendingclub.com/foliofn/browseNotesAj.action?&sortBy=opa&dir=asc&startindex=%s&pagesize=%s' % (start_index,page_size))
            json_data = json.loads(self.br.response().read())
            self.data = json_data['searchresult']['loans']
        except:
            raise Exception('failed to grab data')
        
    def get_data(self):
        return self.data


class NotePageParser(object):
    '''Parses the Note page from foliofn'''
    
    def parse_html(self, html):
        '''
        takes an html string as input and parses it into a JSON doc to be
        inserted into a MongoDB
        '''
        self.db_doc = {}
        soup = BeautifulSoup.BeautifulSoup(html)
        self.parse_summary(soup)
        self.parse_credit_score(soup)
        self.parse_payments(soup)
        self.parse_collections(soup)
        
        return self.db_doc

    def parse_summary(self, soup):
        '''
        parse the summary blocks at the top of the page
        some of the info in superfluous due to crawling note orders and loan pages
        '''
        ioi = ['Loan Fraction', 'Loan Amount', 'Status', re.compile('Last Payment.*',re.I),
               re.compile('Payments to Date.*',re.I), 'Principal','Interest', 
               'Late Fees Received', re.compile('Next Payment.*',re.I),
               re.compile('Remaining Payments.*',re.I), 'Outstanding Principal',
               re.compile('Expected Final Payment.*',re.I)]

        for i in ioi:
            s = soup.find(text=i)
            val = s.findNext('td').text
            h,hval = self.transform_header(s)
            
            if h == 'expected_final_payment':
                val = NotePageParser.mdy_todate(val)
            elif h == 'status':
                val = val
            else:
                val = LoanPageParser.dollars_to_float(val)
             
            self.db_doc[h] = val
    
    def transform_header(self, header):
        header = header.strip().replace('\t','').replace('\n','')
        
        header_val = None
        if '(' in header:
            header, header_val = header.split('(')
            header = header[:-1]
            header_val = header_val.replace(')','')
            if '/' in header_val:
                header_val = NotePageParser.mdy_todate(header_val)
            else:
                header_val = int(header_val)
        
        return header.lower().replace(' ','_'), header_val
    
    def parse_credit_score(self, soup):
        s = soup.find('table', id='trend-data').find('tbody').findAll('tr')
        l = []
        for c in s:
            score_range = c.findAll('td')[0].text
            date = datetime.datetime.strptime(c.findAll('td')[1].text,'%B %d, %Y')
            l.append({'range':score_range,'date':date})
        self.db_doc['credit_score_range'] = l
            
    def parse_payments(self, soup):
        s = soup.find('table', id='lcLoanPerfTable1').find('tbody').findAll('tr')
        pay_docs = []
        for line in s:
            vals = []
            entries = line.findAll('td')
            for i in range(len(entries)):
                e = entries[i].text
                if e == '--':
                    vals.append(None)
                    continue
                elif i <= 1:
                    vals.append(NotePageParser.mdy_todate(e))
                elif '$' in e:
                    vals.append(LoanPageParser.dollars_to_float(e))
                else:
                    vals.append(NotePageParser.clean_str(e))
            pay_docs.append(NotePageParser.payment_subdoc(vals))
        self.db_doc['payment_history'] = pay_docs
    
    def parse_collections(self, soup):
        try:
            s = soup.find('table', id='lcLoanPerfTable2').find('tbody').findAll('tr')
        except:
            # no collections
            return
        
        coll_docs = []
        for line in s:
            t,d = line.findAll('td')
            t = NotePageParser.mdy_todate(t.text.split(' ')[0])
            d = d.text
            coll_docs.append(NotePageParser.collection_subdoc(t,d))
        self.db_doc['collection_log'] = coll_docs
    
    @staticmethod
    def collection_subdoc(t,d):
        doc = {'date':t, 'description':d}
        return doc
    
    @staticmethod
    def payment_subdoc(vals):
        keys = ['due_date','completion_date',
               'amount','principal','interest',
               'late_fees','principal_balance',
               'status']
        
        doc = {}
        for i in range(len(keys)):
            k,v = keys[i], vals[i]
            if v is not None:
                doc[k] = v

        return doc
    
    @staticmethod
    def clean_str(s):
        return s.strip().replace('\t','').replace('\n','')
    
    @staticmethod
    def mdy_todate(s):
        m,d,y = s.split('/')
        date = datetime.datetime(int(y),int(m),int(d),0,0,0)
        return date

class LoanPageParser(object):
    
    def __init__(self):
        '''initializes the parser'''

        self.trans_funcs = {'Amount Requested':LoanPageParser.dollars_to_float,
                            'Loan Purpose': LoanPageParser.identity,
                            'Loan Grade': LoanPageParser.identity,
                            'Interest Rate':LoanPageParser.percent_to_float,
                            'Loan Length':LoanPageParser.loan_length_months,
                            'Monthly Payment':LoanPageParser.monthly_to_float,
                            'Funding Received':LoanPageParser.to_percent_funded,
                            'Investors':LoanPageParser.parse_investors,
                            'Loan Status':LoanPageParser.identity,
                            'Listing Issued on':LoanPageParser.loan_submit_datetime,
                            'Loan Submitted on':LoanPageParser.loan_submit_datetime,
                            'Note:':LoanPageParser.identity,
                            'Home Ownership':LoanPageParser.identity,
                            'Current Employer':LoanPageParser.identity,
                            'Length of Employment':LoanPageParser.identity,
                            'Gross Income':LoanPageParser.monthly_to_float,
                            'Debt-to-Income (DTI)':LoanPageParser.percent_to_float,
                            'Location':LoanPageParser.identity,
                            'Credit Score Range:':LoanPageParser.identity,
                            'Earliest Credit Line':LoanPageParser.credit_since,
                            'Open Credit Lines':int,
                            'Total Credit Lines':int,
                            'Revolving Credit Balance':LoanPageParser.dollars_to_float,
                            'Revolving Line Utilization':LoanPageParser.percent_to_float,
                            'Inquiries in the Last 6 Months':int,
                            'Accounts Now Delinquent':int,
                            'Delinquent Amount':LoanPageParser.dollars_to_float,
                            'Delinquencies (Last 2 yrs)':int,
                            'Months Since Last Delinquency':LoanPageParser.months_since,
                            'Public Records On File':int,
                            'Months Since Last Record':LoanPageParser.months_since
                            }
        
    def parse_html(self, html_str):
        '''
        takes an html string as input and parses it into a JSON doc to be
        inserted into a MongoDB
        '''
        self.db_doc = {}
        soup = BeautifulSoup.BeautifulSoup(html_str)
        
        try:
            self.parse_basics(soup)
        except:
            print soup.prettify()
            raise Exception('Unable to parse basic info')

        self.parse_details(soup)
        
        try:
            self.parse_QA(soup)
        except:
            raise Exception('Unable to parse QA for loanID %s' % self.db_doc['loanID'])
        
        return self.db_doc
    
    def parse_basics(self, soup):
        '''parse basic information like loanID, title and description'''
        loan_text = soup('div', attrs={'class':re.compile("^memberHeader$", re.I)})[0].text
        self.db_doc['loanID'] = int(loan_text.split(' ')[3])
        self.db_doc['title'] = soup.html.head.title.string
        self.db_doc['description'] = soup.findAll('div', id='loan_description')[0].text
        
    def parse_QA(self, soup):
        '''parse the Q&A section of the loan page and insert into DB doc'''
        qs = soup('span', attrs={'class':re.compile("^%squestions-container$" % self.db_doc['loanID'], re.I)})
        ans = soup('div', attrs={'class':re.compile("^answer$", re.I)})

        qas = []
        for i in range(len(qs)):
            q = qs[i].string
            a = ans[i].text
            a = a.replace(ans[i].strong.string.strip(),'')
            t = LoanPageParser.answer_time_to_datetime(ans[i].strong.string)
            qas.append({'question':q, 'answer':a, 'time':t})           
        self.db_doc['QA'] = qas
        
    def parse_details(self, soup):
        '''Parse the details sections of the loan page and insert into DB doc'''
        for i in range(6):
            ld_heads = soup('table', attrs={'class':re.compile("^loan-details$", re.I)})[i].findAll('th')
            ld_vals = soup('table', attrs={'class':re.compile("^loan-details$", re.I)})[i].findAll('td')
            for k in range(len(ld_heads)):
                try:
                    head, val = ld_heads[k].text, ld_vals[k]
                    if head == 'Amount Requested':
                        val = val.div.string
                    elif head == 'Loan Grade':
                        val = val.span.string
                    else:
                        val = val.text
                except:
                    raise Exception('cant parse html correctly')
                
                head, val = self.transform(head,val)
                self.db_doc[head] = val
                
    def transform(self, header, value):
        '''Transform the values parsed from html to what 
        will be inserted into the DB'''
        
        try:
            f = self.trans_funcs[header]
        except:
            raise Exception('No key for: %s (loanID: %s)' % (header, self.db_doc['loanID']))
        
        try:
            if value != 'n/a':
                value = f(value)
        except:
            raise Exception('Function %s doesnt work for loanID, header, value: %s, %s, %s' % (f, self.db_doc['loanID'], header, value))
        
        header = self.reformat_header(header)
        
        return header, value
    
    def reformat_header(self, header):
        '''get rid of spaces and back characters'''
        header = header.lower().replace(' ','_').replace('-','_')
        return header.replace('(','').replace(')','').replace(':','')
    
    @staticmethod
    def answer_time_to_datetime(val):
        val = val.split(' ')
        date,time = val[1].replace('(','').replace(')','').split('-')
        h,min = time.split(':')
        m,d,y = date.split('/')
        return datetime.datetime(int(y),int(m),int(d),int(h),int(min))
    
    @staticmethod
    def dollars_to_float(d_str):
        d_str = d_str.replace('$','')
        d_str = d_str.replace(',','',10)
        return float(d_str)
    
    @staticmethod
    def to_percent_funded(val):
        val = val.split(' ')
        val = val[1][1:]
        return LoanPageParser.percent_to_float(val)
    
    @staticmethod
    def percent_to_float(val):
        val = val.replace('%','')
        return float(val)/100
        
    @staticmethod
    def identity(val):
        return val
    
    @staticmethod
    def months_since(val):
        if val.strip() != 'n/a':
            return int(val)
        else:
            return val
    
    @staticmethod
    def loan_length_months(val):
        val = val.split(' ')
        val = int(val[2].replace('(',''))
        return val
    
    @staticmethod
    def monthly_to_float(val):
        val = val.split(' ')[0]
        return LoanPageParser.dollars_to_float(val)
    
    @staticmethod
    def parse_investors(val):
        return int(val.split(' ')[0])
    
    @staticmethod
    def loan_submit_datetime(val):
        '''in format e.g. 10/6/09 9:57 AM'''
        val = val.strip().split(' ')
        date = val[0].split('/')
        time = val[1].split(':')
        if val[2] == 'PM':
            h = (12 + int(time[0])) % 24
        else:
            h = int(time[0])
        d = datetime.datetime(int(date[2])+2000, int(date[0]), int(date[1]),
                              h, int(time[1]))
        return d
    
    @staticmethod
    def empl_len(val):
        return int(val.split(' ')[0])
    
    @staticmethod
    def credit_since(date):
        '''take the date given for earliest credit line
        and convert to datetime object'''
        m,y = date.split('/')
        return datetime.datetime(int(y),int(m),1)
    

if __name__ == '__main__':
    
    NP = NotePageParser()
    base_url = 'https://www.lendingclub.com/foliofn/loanPerf.action?loan_id=%s&order_id=%s&note_id=%s'
    login_str = 'Only Lending Club investors can sign up as trading members'
    
    PC = PageCrawler(base_url, login_str, login='',pwd='')
    PC.crawl([(376486,2757140,246742)])
    NP.parse_html(PC.get_data()[(376486,2757140,246742)])

                