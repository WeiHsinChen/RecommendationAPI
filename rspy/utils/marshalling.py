from cStringIO import StringIO
import gzip

def to_gzip_b64(str):
    io_out = StringIO()
    f = gzip.GzipFile(mode='w', fileobj=io_out)
    f.write(str)
    f.close()
    ret=io_out.getvalue().encode('base64').rstrip()
    io_out.close()
    return ret

def from_gzip_b64(str):
    ret=None
    try:
        io_in = StringIO(str.decode('base64'))
        f=gzip.GzipFile(mode='rb', fileobj=io_in)
        ret=f.read()
        f.close()
        io_in.close()
    except:
        pass
    return ret


def reply_xml(str):
    m = to_gzip_b64(str)
    return "<rep><data>%s</data></rep>" % m


def reply_xml_data_rep(str):
    m = to_gzip_b64(str)
    return "<data><rep><gzb64>%s</gzb64></rep></data>" % m