'''
This module contains classes that update the DB from various sources.

The three primary sources of data are:
1.) 'Note Orders' from foliofn,
2.) 'Note Pages' from foliofn/LC
3.) 'Loan Pages' from LendingClub

Each of these sources corresponds to a class.

The DB is currently has two collections:
1.) Notes
2.) Loans

Each source/class above may update one or both of these collections at once.

#################
Update Scheduling

In general, the NoteOrders should be updated daily. Loan pages need only be
crawled once and never updated as they never change.  NotePages should be
updated weekly or so, or whenever the NoteOrder changes.
'''

import datetime
from data_scrapers import NoteOrders, PageCrawler, LoanPageParser, NotePageParser
from setup_mongodb import get_db

class NoteOrdersUpdater(object):
    
    def __init__(self):
        self.notes = get_db('lc_db').notes
        self.NO = NoteOrders()

    def update(self):
        print 'Pulling new data from foliofn...'
        self.NO.grab_data(0, 999999)
        print 'Updating DB...'
        for note in self.NO.get_data():
            self.update_note(note)
        print 'Done.'
            
    def update_note(self, note):
        '''
        Insert new note data into the 'notes' collection.
        
        'note' is a JSON style document captured from foliofn
        'note_doc' is created in this method, it is the document
        that is part of the db notes collection
        
        If no note exists in the db, create a new note document.
        If a note exists, update the time series data
        '''
            
        note_doc = self.notes.find_one({'noteID':int(note['noteId'])})
        if note_doc is None:
            self.notes.insert(self.create_note_doc(note))
        else:
            self.update_field(note, note_doc, 'asking_price')
            self.update_field(note, note_doc, 'ytm')
            self.update_field(note, note_doc, 'markup_discount')  
            self.notes.update({'noteID':note['noteId']},
                              {'$set': {'outstanding_principal':float(note['outstanding_principal']),
                                        'accrued_interest':float(note['accrued_interest'])
                                        }
                               }, safe=True
                              )
            
    def update_field(self, note, note_doc, field):
        '''
        Update a field in a note document which is an array
        of subdocuments if the value of the last entry is different
        from the current measurement
        '''

        subdoc = note_doc.get(field,None)
        try:
            if subdoc is not None and subdoc[-1][field] == float(note[field]):
                return
        except ValueError:
            return
        
        self.notes.update({'noteID':int(note['noteId'])},
                          {"$push":{field:{field:float(note[field]),
                                           'time':datetime.datetime.utcnow()
                                           }
                                    }
                           }, safe=True
                          )   
        
    def create_note_doc(self, note):
        '''
        Create a new note document to add to notes collection
        
        'note' is a JSON style document captured from foliofn
        '''
    
        try:
            note_doc = {
                'orderID':int(note['orderId']),
                'noteID':int(note['noteId']),
                'loanID':int(note['loanGUID']),
                'asking_price':NoteOrdersUpdater.create_subdoc('asking_price',note['asking_price']),
                'markup_discount':NoteOrdersUpdater.create_subdoc('markup_discount',note['markup_discount']),
                'ytm':NoteOrdersUpdater.create_subdoc('ytm', note['ytm']),
                'trading_status':True,
                'outstanding_principal':float(note['outstanding_principal']),
                'accrued_interest':float(note['accrued_interest']),
                }
        except:
            raise Exception('unable to create new note document')
        
        return note_doc

    @staticmethod
    def create_subdoc(field, val):
        if val != 'null':
            val = float(val)
        return [{field:val, 'time':datetime.datetime.utcnow()}]

class NotePageUpdater(object):
    '''
    Updates db.loans and db.notes from data gathered
    from crawling the Notes pages
    
    Only fetches a NotePage if the note has changed
    or if the NotePage hasn't been updated in the last
    week.
    '''
    def __init__(self):
        dbh = get_db('lc_db')
        self.notes = dbh.notes
        self.loans = dbh.loans
        
        self.note_page_url = 'https://www.lendingclub.com/foliofn/loanPerf.action?loan_id=%s&order_id=%s&note_id=%s'
        self.login_str = 'Only Lending Club investors can sign up as trading members'
    
    def update(self, wait=2.5, batch_size=1000, days_old=7):
        note_tups = self.note_page_scheduler(days_old) #(loanID,orderID,noteID)

        while len(note_tups)>0:
            # create subset of loans to crawl for
            notes = []
            for i in range(batch_size):
                try:
                    notes.append(note_tups.pop())
                except IndexError:
                    break
            notes_html = self.get_note_pages(notes, wait)
            self.parse_and_insert(notes_html)

    def get_note_pages(self, note_tups, wait):
        PC = PageCrawler(self.note_page_url, self.login_str, wait)    
        PC.crawl(note_tups)
        return PC.get_data()

    def parse_and_insert(self, pages):
        NP = NotePageParser()
        for p in pages:
            loanID,orderID,noteID = p
            try:
                doc = NP.parse_html(pages[p])
            except:
                print 'Failed to parse (loanID: %s,orderID: %s,noteID: %s)' % (loanID,orderID,noteID)
                continue

            # upsert collection log
            try:
                collections = doc['collection_log']
                self.loans.update({'loanID':loanID},
                                  {'$addToSet':{'collection_log':{'$each':collections}}},
                                  upsert=True, safe=True)
            except KeyError:
                pass
  
            # update status
            self.loans.update({'loanID':loanID},
                              {'$set':{'status':doc['status']}},
                              upsert=True, safe=True)     
                 
            ## note ID and amount/fraction --> loan
            note_fraction = {'noteID':noteID, 'loan_fraction':doc['loan_fraction']}
            self.loans.update({'loanID':loanID},
                              {'$addToSet':{'notes':note_fraction}},
                              upsert=True, safe=True)
            # payment --> note
            try:
                collections = doc['payment_history']
                self.notes.update({'noteID':noteID},
                                  {'$addToSet':{'payment_history':{'$each':collections}}},
                                  upsert=True, safe=True)
            except KeyError:
                pass
            
            # credit score history --> loan
            try:
                csh = doc['credit_score_range']
                self.loans.update({'loanID':loanID},
                                  {'$addToSet':{'credit_score_history':{'$each':csh}}},
                                  upsert=True, safe=True)
            except KeyError:
                pass
            
            # summary --> note
            self.notes.update({'noteID':noteID},
                              {'$set':{'last_payment':doc['last_payment'],
                                       'payments_to_date':doc['payments_to_date'],
                                       'principal':doc['principal'],
                                       'interest':doc['interest'],
                                       'late_fees_received':doc['late_fees_received'],
                                       'next_payment':doc['next_payment'],
                                       'remaining_payments':doc['remaining_payments'],
                                       'expected_final_payment':doc['expected_final_payment'],
                                       'outstanding_principal':doc['outstanding_principal'],
                                       }}, upsert=True, safe=True)
            
            # add normalized (total loan amount) payment history to loan
            loan_pay = self.normalize_payments(doc)
            self.loans.update({'loanID':loanID},
                              {'$addToSet':{'payment_history':{'$each':loan_pay}}},
                              upsert=True, safe=True)
            
            #update last updated
            self.notes.update({'noteID':noteID},
                              {'$set':{'last_updated':datetime.datetime.utcnow()}
                               }
                              )
            self.notes.update({'loanID':loanID},
                              {'$set':{'last_updated':datetime.datetime.utcnow()}
                               }, safe=True
                              )
    
    def normalize_payments(self, doc):
        '''
        Takes a JSON doc with parsed html info and transforms the payment
        history amount to be portions of the total loan rather than the note amount
        '''
        
        loan_pay_hist = []
        for pay in doc['payment_history']:
            new_pay = {}
            for k,v in pay.iteritems():
                if isinstance(v,float):
                    norm_v = v/doc['loan_fraction'] * doc['loan_amount']
                    new_pay[k] = round(norm_v,2)
                else:
                    new_pay[k] = v
            loan_pay_hist.append(new_pay)
        return loan_pay_hist
    
    def note_page_scheduler(self, days_old):
        '''
        return a list of (loanID,orderID,noteID) tuples for
        note pages that are out of date or whose note orders
        have recently changed.
        '''
        NO = NoteOrders()
        NO.grab_data(0, 999999)
        np_tups = []
        for note_order in NO.get_data():
            if self.out_of_date(note_order, days_old) or self.order_changed(note_order):
                try:
                    np_tups.append((int(note_order['loanGUID']), 
                                    int(note_order['orderId']),
                                    int(note_order['noteId'])))
                except KeyError:
                    continue
        return np_tups
    
    def out_of_date(self, note_order, days_old):
        '''
        check to see if a note is out of date
        '''
        try:
            loan = self.loans.find_one({'loanID':int(note_order['loanGUID'])})
            if (datetime.datetime.utcnow() - loan['last_updated']).days < days_old:
                return False                   
        except:
            return True
        
        return True
    
    def order_changed(self, note_order):
        '''
        check to see if a note_order has changed
        '''
        fields = ['asking_price','outstanding_principal','days_since_payment','accrued_interest']
        note = self.notes.find_one({'noteID':int(note_order['noteId'])})
        for f in fields:
            try:
                if float(note[f]) != float(note_order[f]):
                    return True
            except:
                return True
        
        return False

class LoanPageUpdater(object):
    '''
    Updates the information for loans in MongoDB by
    fetching data from the LC
    
    Only gets new loan pages that haven't been seen before
    since these pages are static.
    
    Must run NoteOrdersUpdater first to have up-to-date info
    in the DB about which loan pages to grab
    '''
    def __init__(self):
        dbh = get_db('lc_db')
        self.notes = dbh.notes
        self.loans = dbh.loans
        
        self.loan_page_url = 'https://www.lendingclub.com/browse/loanDetail.action?loan_id=%s'
        self.loan_page_login_str = 'This information is only accessible once you register as an Investor'
        
        self.NO = NoteOrders()

    def update(self, wait=2.5, batch_size=1000):              
        self.get_new_loan_pages(wait, batch_size)
                        
    def get_new_loan_pages(self, wait, N):
        LPP = LoanPageParser()
        loanids = self.new_loans_set()

        # pull N loan pages at a time and insert into DB
        counter = 0
        num_loanids = len(loanids)
        while len(loanids)>0:
            LC = PageCrawler(self.loan_page_url, self.loan_page_login_str, wait)
            
            # create subset of loans to crawl for
            loans_to_grab = []
            for i in range(N):
                try:
                    loans_to_grab.append(loanids.pop())
                except IndexError:
                    break
                
            LC.crawl(loans_to_grab)
            loans = LC.get_data()
           
            # parse loan and insert into DB
            for loanID in loans:
                html = loans[loanID]
                db_doc = LPP.parse_html(html)
                self.loans.update({'loanID':db_doc['loanID']},
                                  {'$set': db_doc}, upsert=True, safe=True)
                counter += 1
            print 'inserted loan %s of %s' % (counter, num_loanids)
        
    def new_loans_set(self):
        '''
        create a set of loanIDs whose pages have not
        already been crawled
        '''
        # get unique set of loanIDs from notes
        loanids = set()
        for note in self.notes.find():
            try:
                loanids.add(note['loanID'])
            except KeyError:
                print note
        loanids = list(loanids)
        
        # remove loans already in loan DB
        loans_already = []
        for l in self.loans.find():
            loans_already.append(l['loanID'])
    
        for loanID in loans_already:
            if loanID in loanids:
                loanids.remove(loanID)
        
        print 'Retrieving %s loan pages' % len(loanids)
        
        return loanids


if __name__ == '__main__':
       
    print 'Updating Note Orders...'
    NOU = NoteOrdersUpdater()
    NOU.update()
    
    print 'Updating Loan Pages...'
    LPU = LoanPageUpdater()
    LPU.update(batch_size=300)
    
    print 'Updating Note Pages'
    NPU = NotePageUpdater()
    NPU.update(batch_size=300, days_old=5)
    

    



    
