#!/usr/bin/python
import curses
import time
import threading
def show_progress(win,X_line,sleeping_time):
    pos = 10
    for i in range(15):
        win.addstr(X_line,pos,".")
        win.refresh()
        time.sleep(sleeping_time)
        pos += 1
    win.addstr(X_line,26,"Done!")
    win.refresh()
    time.sleep(0.5)
def show_progress_A(win):
    show_progress( win, 1, 0.1)
def show_progress_B(win):
    show_progress( win, 4 , 0.5)
if __name__ == '__main__':
    curses.initscr()
    win = curses.newwin(6,32,14,10)
    win.border(0)
    win.addstr(1,1,"Progress ")
    win.addstr(4,1,"Progress ")
    win.refresh()
    threading.Thread( target = show_progress_B, args = (win,) ).start()
    time.sleep(2.0)
    threading.Thread( target = show_progress_A, args = (win,)).start()

