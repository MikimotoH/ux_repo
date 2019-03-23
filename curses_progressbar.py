import curses
import time

curses.initscr()

def percentage():
    win = curses.newwin(3, 32, 3, 30)
    win.border(0)
    loading = 0
    while loading < 100:
        loading += 1
        time.sleep(0.03)
        update_progress(win, loading)

def update_progress(win, progress):
    rangex = (30 / float(100)) * progress
    pos = int(rangex)
    display = '#'
    if pos != 0:
        win.addstr(1, pos, "{}".format(display))
        win.refresh()

percentage()

curses.endwin()
