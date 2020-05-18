import tkinter
import csv
import pandas as pd
import dask.dataframe as ddf
import os
from tkinter import *
from tkinter import filedialog,messagebox,Frame

def input():
    global filez
    filez = filedialog.askopenfilenames(parent=root,title='Choose a file',filetypes=(("csv files", "*.csv"), ("all files", "*.*")))
    global filenames
    filenames=list(filez)
    messagebox.showinfo("Status", "Task Completed")
    return filenames


def unique():
    global sample
    sample = filedialog.askopenfilenames(parent=root,title='Choose a file',filetypes=(("csv files", "*.csv"), ("all files", "*.*")) )
    global uniq
    uniq = ''.join(sample)
    messagebox.showinfo("Status", "Task Completed")
    return uniq
    # root.filename = filedialog.askopenfilename(initialdir="/", title="Samples",filetypes=(("csv files", "*.csv"), ("all files", "*.*")))
    # print(root.filename)
    # messagebox.showinfo("Status", "Task Completed")
    # text1 = Text(root, bg="Moccasin", width=50,height=10, font="Times")
    # text1.grid()
    # text1.insert(INSERT,root.filename)


def Process():
    d= messagebox.showinfo("Process Time", "please press OK until the next message comes and DO NOT CLOSE THE MAIN WINDOW...")
    df = ddf.read_csv(filenames, dtype=str,keep_default_na=False)
    dfMAIN = pd.read_csv(uniq)
    # for cols in list(df.columns):
    #     if ":" in cols:
    #         # df_by_pd[cols] = df_by_pd[cols].apply(lambda x: str(x))
    #         df = df.astype({cols: 'object'})

    #1covert all column to object
    #2pass it to read csv
    #3take header of second dataframe and pass it on to merge(on)
    #4merge based on column it should not chnage its position
    # for i in df.columns:
    #     print(i)
    # print(df.columns)
    # df=df.astype(str).dtypes
    #
    # dfMAIN=dfMAIN.astype(str).dtypes
    global col
    col = dfMAIN.columns
    col = list(col)
    global merg
    merg = ddf.merge(df,dfMAIN,on=col).compute()
    merg.info()
    print("Processed")
    merg.to_csv('Easy.csv', index=False,encoding="utf-8-sig", sep=",",quoting=csv.QUOTE_ALL,header=True)#line_terminator='\n'
    messagebox.showinfo("Your file has been saved in the given Address", os.getcwd())
    return merg


def Report():
    script_dir = os.getcwd()
    file = 'Easy.csv'

    df1 = pd.read_csv(os.path.normcase(os.path.join(script_dir, file)),low_memory=False)
    df2 = pd.read_csv(uniq, low_memory=False)
    print(df1.shape)
    print(df2.shape)
    # result=pd.merge(df1,df2,on=col,how='outer')
    print(col)
    result=df2[~df2[col].isin(df1[col])]

    # print(f1[~f1.column1.isin(f2.column1)])
    d=int(result[col].count())
    print("count of d",d)
    if(d==0):
        messagebox.showinfo("Keys found","All keys found")

    else:
        result.to_csv('Result.csv',index=False)
        messagebox.showinfo("Mismatched keys found..\n Please find the report",os.getcwd())


def Visualize():
    print("hello")


def about():
    window = tkinter.Toplevel()
    window.geometry("1500x1500")
    window.title("ABOUT")
    photo1 = PhotoImage(file="About.png")
    label = Label(window, image=photo1, bg="white")
    label.photo = photo1
    label.grid(row=0, column=2, sticky=W + E + N + S)
    root.mainloop()



class Application(Frame):
    def __init__(self, master=None):
        Frame.__init__(self, master)
        self.grid()
        self.master.title("LIGATER")

        for r in range(6):
            self.master.rowconfigure(r, weight=1)
        for c in range(5):
            self.master.columnconfigure(c, weight=1)


        Frame1 = Frame(master, bg="blue")
        Frame1.grid(row = 0, column = 0, rowspan = 2,columnspan=1, sticky = W+E+N+S)
        Frame2 = Frame(master, bg="blue")
        Frame2.grid(row = 2, column = 0, rowspan = 4,columnspan=1, sticky = W+E+N+S)
        Frame3 = Frame(master, bg="white")
        Frame3.grid(row = 0, column = 1, rowspan = 10, columnspan = 16, sticky = W+E+N+S)
        # labelframe = LabelFrame(Frame3,text="Cognizant")
        # labelframe.grid()

        label1 = Button(Frame1, text="Files", width=32, height=3,bg='blue', fg="white",justify="center",font=("Bookman Old Style",10,"bold"),command=input)
        label1.grid(sticky=W+E+N+S)
        label2 = Button(Frame1, text="Unique Samples", width=32, height=3, bg='blue', fg="white",justify="center",font=("Bookman Old Style",10,"bold"),command=unique)
        label2.grid(sticky=W+E+N+S)
        # label3 = Button(Frame1, text="Upload", width=32, height=3,bg='blue', fg="white",justify="center",font=("Bookman Old Style",10,"bold"),command=upload)
        # label3.grid(sticky=W+E+N+S)
        label4 = Button(Frame1, text="Progress", width=32, height=3, bg='blue', fg="white",justify="center",font=("Bookman Old Style",10,"bold"),command=Process)
        label4.grid(sticky=W+E+N+S)
        label5 = Button(Frame1, text="Report", width=32, height=3, bg='blue', fg="white",justify="center",font=("Bookman Old Style",10,"bold"),command=Report)
        label5.grid(sticky=W+E+N+S)
        label5 = Button(Frame1, text="Visualization", width=32, height=3, bg='blue', fg="white", justify="center",
                        font=("Bookman Old Style", 10, "bold"), command=Visualize)
        label5.grid(sticky=W + E + N + S)
        label6 = Button(Frame1, text="Exit", width=32, height=3, bg='blue', fg="white", justify="center",
                        font=("Bookman Old Style", 10, "bold"), command=root.quit)
        label6.grid(sticky=W + E + N + S)
        label7 = Button(Frame1, text="About", width=32, height=3, bg='blue', fg="white", justify="center",
                        font=("Bookman Old Style", 10, "bold"), command=about)
        label7.grid(sticky=W + E + N + S)


        photo = PhotoImage(file="Logo1.png")
        label = Label(Frame3, image=photo, bg="white")
        label.photo = photo
        label.grid(row = 0, column = 1,sticky=W+E+N+S)

        # photo1 = PhotoImage(file="Cap2_v1.png")
        # label1 = Label(Frame2, image=photo1, bg="white",justify="right")
        # label1.photo = photo1
        # label1.grid()

root = Tk()
root.geometry("1500x1500")
app = Application(master=root)
app.mainloop()