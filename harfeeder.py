#!/usr/bin/env python
# coding: utf8

"""

BrowserMobProxy very slow:


Ok. I've managed to track this issue.
If temp folder doesn't have userAgentString.properties file

It assume cache need to be updated. In this case it tries to download results from:
http://user-agent-string.info/rpc/get_data.php?key=free&format=ini
and check version from:
http://user-agent-string.info/rpc/get_data.php?key=free&format=ini&ver=y

It looks like sometimes this server is down. which causes long replies. And it looks like data cannot be downloaded - it is requested on every request.

Workaround:
create userAgentString.properties file in temp folder with following content:
lastUpdateCheck=200000000000000
currentVersion=1

This will prevent cache to be updated.

and create file userAgentString.txt with empty content.

Such actions will produce lot of exceptions in console. but at least it will be working.

To my humble opinion this code of UA parsing is piece of crap. I spent whole day trying to understand why proxy stopped working. And found that cause is piles of unmaintainable unstable and unreliable code :(



"""


from selenium import webdriver
from selenium.common.exceptions import WebDriverException, NoSuchElementException
from urllib import urlencode
import httplib
import time, os, sys, string, multiprocessing, traceback, shutil, StringIO, thread
from PIL import Image
import socket, errno, signal, subprocess
from pprint import pprint
import json
import planparser
import re

import ConfigParser
config = ConfigParser.ConfigParser()

VERBOSE = True

RUNNING_TAGs = set()

def html2text(data):
    # remove the newlines
    data = data.replace("\n", " ")
    data = data.replace("\r", " ")

    # replace consecutive spaces into a single one
    data = " ".join(data.split())

    # get only the body content
    bodyPat = re.compile(r'<body[^<>]*?>(.*?)</body>', re.I)
    result = re.findall(bodyPat, data)
    if result:
        data = result[0]

    # now remove the java script
    p = re.compile(r'<script[^<>]*?>.*?</script>')
    data = p.sub('', data)

    # remove the css styles
    p = re.compile(r'<style[^<>]*?>.*?</style>')
    data = p.sub('', data)

    # remove html comments
    p = re.compile(r'')
    data = p.sub('', data)

    # remove all the tags
    p = re.compile(r'<[^<]*?>')
    data = p.sub('', data)

    return data


def kill_process(process_id):
    try:
        os.kill(process_id, 9)
        return True
    except OSError:
        return False

def kill_process_group(pg_id):
    try:
        os.killpg(pg_id, 9)
        return True
    except OSError:
        return False


def kill_child_processes(parent_pid, sig=signal.SIGTERM):
    ps_command = subprocess.Popen("ps -o pid --ppid %d --noheaders" % parent_pid, shell=True, stdout=subprocess.PIPE)
    ps_output = ps_command.stdout.read()
    retcode = ps_command.wait()
    if retcode == 0:
        for pid_str in ps_output.split("\n")[:-1]:
            os.kill(int(pid_str), sig)

class HttpRequest():

    def __init__(self, name, hostname, port):
         self.hostname = hostname
         self.port = port
         self.name = name

    def send(self, method, path, body=None, headers=None):
        try:
            connection = httplib.HTTPConnection(self.hostname, self.port)
            if body is not None and headers is not None:
                connection.request(method, path, body, headers)
            else:
                connection.request(method, path)
        except socket.error as e:
            if e.errno == errno.ECONNREFUSED:
                raise Exception, self.name + " not reachable on " + self.hostname + " " + str(self.port)
            else:
                raise

        response = connection.getresponse().read()
        connection.close()
        return response

class BrowserMobProxy():

    def __init__(self, proxy_api_host, proxy_api_port):
         self.http_request = HttpRequest("BrowserMobProxy", proxy_api_host, proxy_api_port)


    def create_proxy(self, proxy_port):
        # Base URL for API requests
        self.base_url = "/proxy/" + str(proxy_port)

        # Proxy initialization via REST API
        path = "/proxy?port=" + str(proxy_port)
        self.http_request.send("POST", path)


    def create_har(self, page_id):
        parameters = {"initialPageRef": page_id, "captureHeaders": "true", "captureContent": "false"}
        path = self.base_url + "/har?" + urlencode(parameters)
        self.http_request.send("PUT", path)


    def fetch_har(self):
        path = self.base_url + "/har"
        return self.http_request.send("GET", path)


    def limit_network(self, bw_down, bw_up, latency):
        parameters = {"upstreamKbps": bw_up, "downstreamKbps": bw_down, "latency": latency}
        path = self.base_url + "/limit?" + urlencode(parameters)
        self.http_request.send("PUT", path)

    def terminate(self):
        path = self.base_url
        self.http_request.send("DELETE", path)

    def update_har(self, har, update_dict):
        data = json.loads(har)
        data.update(update_dict)
        return json.dumps(data)

class HarStorage():

    def __init__(self, host, port):
        self.http_request = HttpRequest("HarStorage", host, port)

    def save(self, hars):

        if not hars:
            return

        path = "/results/upload"
        headers = {"Content-type": "application/x-www-form-urlencoded", "Automated": "true"}
        body = {}

        if len(hars) == 1:
            body["file"] = hars[0]
        else:
            body["multi_file"] = json.dumps(hars)

        body = urlencode(body)

        return self.http_request.send("POST", path, body, headers)



class Firefox():

    def __init__(self):
        profile = config.get("browsermob", "firefox_profile")
        if profile:
            self.profile = webdriver.FirefoxProfile(profile)
        else:
            self.profile = webdriver.FirefoxProfile()

    def set_proxy(self, proxy_host, proxy_port):
        self.profile.set_preference("network.proxy.http", proxy_host)
        self.profile.set_preference("network.proxy.http_port", int(proxy_port))
        self.profile.set_preference("network.proxy.type", 1)
        self.profile.set_preference("extensions.showMismatchUI", False)
        self.profile.set_preference("app.update.auto", False)
        self.profile.set_preference("app.update.enabled", False)
        self.profile.set_preference("browser.search.update", False)
        self.profile.set_preference("extensions.update.enabled", False)
        self.profile.set_preference("general.useragent.override", "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:15.0) Gecko/20100101 Firefox/15.0.1");

        self.profile.update_preferences()

    def launch(self):
        ffbin_path = config.get("config", "firefox_bin")
        if ffbin_path:
            ffbin = webdriver.firefox.firefox_binary.FirefoxBinary(ffbin_path)
        else:
            ffbin = None
        self.driver = webdriver.Firefox(firefox_profile = self.profile, firefox_binary = ffbin)


class AutomationScript(object):

    def __init__(self, tag, label, url, visual, driver, bmp, timeout_shared, timeout, do_screenshot):

        if url.startswith("script"):
            self.url = None
            self.visual = None
            self.script_name = url.split()[1]
            self.args = url.split()[2:]
            self.flow = None
        else:
            self.url = url
            self.visual = visual
            self.script_name = None
            self.args = None
            self.flow = None

        self.tag = tag
        self.label = label
        self.timeout_shared = timeout_shared
        self.timeout = timeout
        self.driver = driver
        self.bmp = bmp
        self.do_screenshot = do_screenshot

        self.hars = []


    def setup(self):

        if self.script_name:
            script_path = config.get("scripts", "path") + self.script_name
            f = open(script_path, "rb")
            script_src = f.read()
            f.close()

            self.globals = {"ScreenshotDriver": AutomationScriptContextManager(automation_script = self, do_screenshot = True),
                            "Driver": AutomationScriptContextManager(automation_script = self, do_screenshot = False),
                            "__script_path__": os.path.dirname(os.path.abspath(script_path))}

            exec script_src in self.globals

            self.flow = self.globals["flow"]


    def execute(self):
        if self.url:
            with AutomationScriptContextManager(automation_script = self, do_screenshot = self.do_screenshot) as driver:
               driver.get(self.url)
        elif self.flow:
            try:
                self.flow()
            except:
                if VERBOSE:
                    print
                    print "*** Error while handling click-path:"
                    traceback.print_exc()
                    print

    def get_url(self):
        return self.driver.current_url

    def add_har(self, har):
        self.hars.append(har)

    def get_hars(self):
        return self.hars

    def reset_timeout(self):
        self.timeout_shared.value = self.timeout # next round gets another timeout!




class AutomationScriptContextManager(object):

    def __init__(self, automation_script, do_screenshot):
        self.automation_script = automation_script
        self.do_screenshot = do_screenshot

        self.reset()

        time.sleep(1) # selenium grace time
        self.automation_script.bmp.create_har("dummy")
        time.sleep(1) # selenium grace time

    def reset(self):
        self.error_msg = ""
        self.info_msg = []


    def info(self, msg):
        if type(msg) is str:
            print 'Warning: Info-string is not unicode. Please set encoding of file and use u"..."-string constants!'

        self.info_msg.append(msg)

    def is_error_page(self):
        """ Returns true, if the page we are on is an firefox-error page """
        return self.automation_script.driver.title == "Problem loading page"

    def wait_for_page(self, url, timeout):
        """ waits until timeout, or the browser reaches the url """

        while (self.automation_script.driver.current_url != url) and (timeout >= 0):
            time.sleep(0.1)
            timeout -= 0.1

        if timeout < 0:
            return "timeout"
        elif (self.automation_script.driver.current_url != url) or self.is_error_page():
            return "error"
        else:
            return "ok"


    def has_element(self, by = "id", value = None):
        """ Gracefully tries to find an element """
        try:
            el = self.automation_script.driver.find_element(by = by, value = value)
            return el
        except NoSuchElementException:
            return None

    def click_option(self, select_element, value):
        """ Selects from the select_element the specific option """

        for option in select_element.find_elements_by_tag_name("option"):
            if option.get_attribute("value") == value:
                option.click()
                return True
            elif option.text == value:
                option.click()
                return True

        return False

    def assert_text_on_page(self, text, timeout = 0):
        """ Checks whether a text is on page or not! """

        while (timeout >= 0) and (text not in html2text(self.automation_script.driver.page_source)):
            time.sleep(0.1)
            timeout -= 0.1

        if text not in html2text(self.automation_script.driver.page_source):
            raise AssertionError, "Text '" + text + "' not found in current page!"

    def wait_for_text(self, text, timeout):
        """ waits until timeout, or the text is on the page """

        while (timeout >= 0) and (text not in html2text(self.automation_script.driver.page_source)):
            time.sleep(0.1)
            timeout -= 0.1

        if text not in html2text(self.automation_script.driver.page_source):
            return "ok"
        else:
            return "timeout"

    def wait_for_link(self, text, timeout):
        """ waits until timeout, or the link with this text is on the page """

        link_el = None
        while (not link_el) and (timeout >= 0):
            try:
                link_el = self.automation_script.driver.find_elements_by_link_text(text)
            except:
                link_el = None
                time.sleep(1)
            timeout -= 1

        return link_el[0]



    def fill_form(self, data):
        """ Fill a complete form """

        for locator, value in data.items():

            locator_type, locator_id = locator.split(":")
            locator_id = locator_id.strip()

            form_item = self.automation_script.driver.find_element(by = locator_type, value = locator_id)
            if form_item.tag_name == "select":
                self.click_option(form_item, value)
            elif form_item.get_attribute("type") == "checkbox":
                if form_item.is_selected() != value:
                    form_item.click()
            elif form_item.get_attribute("type") == "file":
                form_item.send_keys(value)
            else:
                form_item.clear()
                form_item.send_keys(value)

    def dismiss_alert(self):
        """ If an alert is popped up, dismiss it and return "dismissed".
        If no alert is popped up: return "no_alert"
        """

        alert_box = self.automation_script.driver.switch_to_alert()
        try:
            alert_box.accept()
            self.automation_script.driver.switch_to_default_content()
            return "dismissed"
        except:
            pass

        try:
            self.automation_script.driver.switch_to_default_content()
        except:
            pass

        return "no_alert"


    def __enter__(self):

        self.automation_script.reset_timeout()
        self.automation_script.bmp.create_har(self.automation_script.label)
        self.error_msg = ""

        self.automation_script.driver.info = self.info
        self.automation_script.driver.wait_for_page = self.wait_for_page
        self.automation_script.driver.wait_for_text = self.wait_for_text
        self.automation_script.driver.wait_for_link = self.wait_for_link
        self.automation_script.driver.is_error_page = self.is_error_page
        self.automation_script.driver.has_element = self.has_element
        self.automation_script.driver.click_option = self.click_option
        self.automation_script.driver.assert_text_on_page = self.assert_text_on_page
        self.automation_script.driver.fill_form = self.fill_form
        self.automation_script.driver.dismiss_alert = self.dismiss_alert



        return self.automation_script.driver

    def __exit__(self, type, value, traceback):

        time.sleep(1) # selenium grace time

        if isinstance(value, WebDriverException):
            self.error_msg = str(value)


        # Read data from container
        har = self.automation_script.bmp.fetch_har()

        har_update = {"tag": self.automation_script.tag,
                      "visual": self.automation_script.visual}

        if self.info_msg:
            har_update["info"] = string.join(self.info_msg, "")

        if self.error_msg:
            har_update["error_msg"] = self.error_msg
            har_update["url"] = self.automation_script.get_url()

        # screenie
        if self.do_screenshot:

            in_buf = StringIO.StringIO()
            in_buf.write(self.automation_script.driver.get_screenshot_as_base64().decode("base64"))
            in_buf.seek(0)
            i = Image.open(in_buf)
            out_buf = StringIO.StringIO()
            i.save(out_buf, format="JPEG", quality=15)
            screen_data = out_buf.getvalue().encode("base64")

            har_update["screenshot"] = screen_data

        har = self.automation_script.bmp.update_har(har, har_update)

        # remember har
        self.automation_script.add_har(har)

        # reset
        self.reset()





def try_dump(tag, label, url, do_screenshot, verbose, proxy_port, profile_browser_path, timeout_shared, timeout, rc_shared):
    try:
        dump(tag, label, url, do_screenshot, verbose, proxy_port, profile_browser_path, timeout_shared, timeout)
        rc_shared.value = "ok"
    except:
        rc_shared.value = "error"
        if verbose:
            raise

def dump(tag, label, url, do_screenshot, verbose, proxy_port, profile_browser_path, timeout_shared, timeout):
    # BrowserMob Proxy constructor
    bmp = BrowserMobProxy(config.get("browsermob", "proxy_api_host"),
                          config.get("browsermob", "proxy_api_port"))

    # Temporary proxy initialization
    bmp.create_proxy(proxy_port)


    # Change browser settings
    firefox = Firefox()
    profile_browser_path.value = firefox.profile.profile_dir

    firefox.set_proxy(config.get("proxy", "proxy_host"),
                      proxy_port)

    try:
        firefox.launch()

        # Network emulation
        if config.get("network", "limit_network") == "True":
            bmp.limit_network(config.get("network", "downstream_kbps"),
                              config.get("network", "upstream_kbps"),
                              config.get("network", "latency_ms"))

        if type(url) is tuple:
            url, visual = url
        else:
            visual = None

        script = AutomationScript(tag = tag,
                                  url = url,
                                  visual = visual,
                                  label = label,
                                  do_screenshot=do_screenshot,
                                  driver=firefox.driver,
                                  bmp=bmp,
                                  timeout_shared=timeout_shared,
                                  timeout=timeout)
        script.setup()
        script.execute()

        # Send results to HAR Storage
        harstorage = HarStorage(config.get("harstorage", "harstorage_host"),
                                config.get("harstorage", "harstorage_port"))
        rc = harstorage.save(script.get_hars())


        if verbose:
            if rc == "Successful":
                print "har saved."
            else:
                print "Could not save the HAR to HarStorage"



    finally:
        # Close the browser
        if hasattr(firefox, "driver") and firefox.driver:
            firefox.driver.quit()

        # Terminate proxy
        bmp.terminate()

    return rc



def secure_dump(tag, label, url, do_screenshot, timeout, verbose, proxy_port, priority):

    # priority-handling
    if priority == "low":
        # runs only if no other task is running
        while RUNNING_TAGs:
            time.sleep(0.5)
        RUNNING_TAGs.add(tag)
    elif priority == "exclusive":
        # waits until all other tasks are ready, locking other tasks to start
        RUNNING_TAGs.add("__exclusive__")
        while len(RUNNING_TAGs) > 1:
            time.sleep(0.5)
        RUNNING_TAGs.add(tag)
    else:
        # "normal" tasks only wait for "exclusive" tasks
        while "__exclusive__" in RUNNING_TAGs:
            time.sleep(0.5)
        RUNNING_TAGs.add(tag)

    browser_profile_path = multiprocessing.Array("c", 256)
    timeout_shared = multiprocessing.Value("i", timeout)
    rc_shared = multiprocessing.Array("c", 256)

    p = multiprocessing.Process(target=try_dump, args=(tag, label, url, do_screenshot, verbose, proxy_port, browser_profile_path, timeout_shared, timeout, rc_shared))
    p.start()
    pid = p.pid

    while timeout_shared.value > 0:
        timeout_shared.value -= 1
        time.sleep(1)
        if not p.is_alive():
            break

    kill_child_processes(pid)
    time.sleep(2.5)
    kill_child_processes(pid, signal.SIGKILL)
    time.sleep(2.5)
    p.terminate()

    if browser_profile_path.value:
        path = browser_profile_path.value
        path = path.replace("/webdriver-py-profilecopy", "")
        if path:
            shutil.rmtree(path, ignore_errors = True)

    RUNNING_TAGs.remove(tag)

    if priority == "exclusive":
        RUNNING_TAGs.remove("__exclusive__")

    return rc_shared.value




def is_plan_scheduled(plan_info, ts):
    min = time.localtime(ts).tm_min
    hour = time.localtime(ts).tm_hour
    wday = time.localtime(ts).tm_wday

    if not (plan_info["min"] or plan_info["hour"] or plan_info["wday"]):
        return True

    if plan_info["min"] and (min % plan_info["min"] == 0):
        return True

    if plan_info["hour"] and (hour % plan_info["hour"] == 0):
        return True

    if plan_info["wday"] and (wday % plan_info["wday"] == 0):
        return True

    return False

def handle_plan(url, plan_info):

    verbose = config.get("config", "verbose") == "True"


    if verbose:
        print "[" + time.ctime() + "] checking plan '" + plan_info["label"] + "': ",

    if plan_info["delay"]:
        if verbose:
            print "delay plan for ", plan_info["delay"], " secs..."
        time.sleep(plan_info["delay"])

    if verbose:
        print time.ctime(), "url:", url

    for cnt in range(plan_info["retries"] + 1):
        rc = secure_dump(tag=plan_info["tag"],
                         label=plan_info["label"],
                         url=url,
                         do_screenshot = plan_info["screenshot"],
                         timeout = plan_info["timeout"],
                         verbose = verbose,
                         proxy_port = plan_info["proxy_port"],
                         priority = plan_info["priority"])
        if rc == "ok":
            break
        elif verbose:
            print time.ctime(), "retry:", url

    time.sleep(1)

def worker_thread(name, queue):
    while True:
        if queue:
            work = queue.pop(0)
            url = work["url"]
            plan_info = work["plan_info"]
            print "got work:", url

            try:
                handle_plan(url, plan_info)
            except:
                traceback.print_exc()

        else:
            time.sleep(5)

if __name__ == "__main__":

    if len(sys.argv) == 1:
        print "*** Usage:"
        print "python harfeeder.py config-file"
        print
        sys.exit(0)

    config.read("defaults.conf")
    config.read(sys.argv[1])

    # setting DISPLAY-Variable for selenium/firefox action
    os.environ["DISPLAY"] = config.get("config", "display")

    # Chaning to the config-path
    os.chdir(os.path.dirname(os.path.abspath(sys.argv[1])))
    sys.path.append(os.path.dirname(os.path.abspath(sys.argv[1])))


    work_queues = {}
    plans = config.items("plan")
    for name, plan in plans:
        work_queues[name] = []
        thread.start_new_thread(worker_thread, (name, work_queues[name],))

    while True:

        # one minute scheduler...
        ts = time.time()

        urls_to_handle = set()

        idx = 0
        for name, plan in plans:
            plan_info = planparser.parse_plan(name, plan, config, idx)
            idx += 1
            plan_info["tag"] = name
            work_queue = work_queues[name]
            if is_plan_scheduled(plan_info, ts):
                for url in plan_info["urls"]:
                    if url not in urls_to_handle:
                        urls_to_handle.add(url)
                        already_enqueued = False
                        for work in work_queue:
                            if work["url"] == url:
                                already_enqueued = True
                        if not already_enqueued:
                            work_queue.append({"url": url, "plan_info": plan_info})

        if config.get("config", "keep_running") == "False":
            break

        sec = time.localtime().tm_sec
        time.sleep(60 - sec)




