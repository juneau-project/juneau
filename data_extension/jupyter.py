import subprocess
import sys
import site
import logging
logging.basicConfig(level=logging.DEBUG)

# Execute via IPython kernel
def exec_ipython(kernel_id, search_var, py_file):
    logging.debug('Exec ' + py_file)
    file_name = site.getsitepackages()[0] + '/data_extension/' + py_file + '.py'
    try:
        msg_id = subprocess.Popen(['python', file_name, \
                                   kernel_id, search_var], \
                                  stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    except FileNotFoundError:
        msg_id = subprocess.Popen(['python3', file_name, \
                                   kernel_id, search_var], \
                                  stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    output, error = msg_id.communicate()

    if sys.version[0] == '3':
        output = output.decode("utf-8")
        error = error.decode("utf-8")
    output = output.strip('\n')

    msg_id.stdout.close()
    msg_id.stderr.close()

    logging.debug(output)

    return output, error
