define([
    'require',
    'jquery',
    'base/js/namespace',
    'base/js/events',
    'notebook/js/codecell',
    'notebook/js/notebook',
    'base/js/utils'
], function(
    requirejs,
    $,
    Jupyter,
    events,
    codecell,
    nb,
    utils
) {
    "use strict";

    var mod_name = "dataset_inspector";
    var log_prefix = '[' + mod_name + '] ';


    // ...........Parameters configuration......................
    // define default values for config parameters if they were not present in general settings (notebook.json)
    var cfg = {
        'window_display': false,
        'cols': {
            'lenName': 24,
            'lenType': 16,
            'lenVar': 40
        },
        'kernels_config' : {
            'python': {
                library: 'var_list.py',
                delete_cmd_prefix: 'del ',
                delete_cmd_postfix: '',
                varRefreshCmd: 'print(var_dic_list())'
            },
            'r': {
                library: 'var_list.r',
                delete_cmd_prefix: 'rm(',
                delete_cmd_postfix: ') ',
                varRefreshCmd: 'cat(var_dic_list()) '
            }
        },
        'types_to_exclude': ['module', 'function', 'builtin_function_or_method', 'instance', '_Feature']
    }

    function execute_code(code, cell) {
        if (!cell.kernel) {
            console.log(i18n.msg._("Can't execute cell since kernel is not set."));
            return;
        }

        cell.clear_output(false, true);
        var old_msg_id = cell.last_msg_id;
        cell.set_input_prompt('*');
        cell.element.addClass("running");
        var callbacks = cell.get_callbacks();

        cell.last_msg_id = cell.kernel.execute(code, callbacks, {silent: false, store_history: true});
        cell.render();
        cell.events.trigger('execute.CodeCell', {cell: this});
        var that = cell;
        function handleFinished(evt, data) {
            if (that.kernel.id === data.kernel.id && that.last_msg_id === data.msg_id) {
                    that.events.trigger('finished_execute.CodeCell', {cell: that});
                that.events.off('finished_iopub.Kernel', handleFinished);
              }
        }
        cell.events.on('finished_iopub.Kernel', handleFinished);
        cell.set_text("#Tables Imported");

    };

    //.....................global variables....

    var st = {}
    st.config_loaded = false;
    st.extension_initialized = false;
    st.code_init = "";

    function read_config(name, cfg, callback) { // read after nb is loaded
        var config = Jupyter.notebook.config;
        config.loaded.then(function() {
            // config may be specified at system level or at document level.
            // first, update defaults with config loaded from server
            cfg = $.extend(true, cfg, config.data[name]);
            // then update cfg with some vars found in current notebook metadata
            // and save in nb metadata (then can be modified per document)

            // window_display is taken from notebook metadata
            if (Jupyter.notebook.metadata[name]) {
                if (Jupyter.notebook.metadata[name].window_display)
                    cfg.window_display = Jupyter.notebook.metadata[name].window_display;
            }

            cfg = Jupyter.notebook.metadata[name] = $.extend(true,
            cfg, Jupyter.notebook.metadata[name]);

            // but cols and kernels_config are taken from system (if defined)
            if (config.data[name]) {
                if (config.data[name].cols) {
                    cfg.cols = $.extend(true, cfg.cols, config.data[name].cols);
                }
                if (config.data[name].kernels_config) {
                    cfg.kernels_config = $.extend(true, cfg.kernels_config, config.data[name].kernels_config);
                }
            }

            // call callbacks
            callback && callback();
            st.config_loaded = true;
        })
        return cfg;
    }

    var sortable;

    function toggleVarInspector() {
        toggle_dataset_inspector(cfg, st)
    }

    function toggleSearch(data){
        toggle_search(data.vname, cfg, st);
    }

    var dataset_inspector_button = function() {
        if (!Jupyter.toolbar) {
            events.on("app_initialized.NotebookApp", dataset_inspector_button);
            return;
        }
        if ($("#dataset_inspector_button").length === 0) {
            $(Jupyter.toolbar.add_buttons_group([
                Jupyter.keyboard_manager.actions.register ({
                    'help'   : 'Variable Inspector',
                    'icon'   : 'fa-crosshairs',
                    'handler': toggleVarInspector,
                }, 'toggle-variable-inspector', 'dataset_inspector')
            ])).find('.btn').attr('id', 'dataset_inspector_button');
        }
    };

    var load_css = function() {
        var link = document.createElement("link");
        link.type = "text/css";
        link.rel = "stylesheet";
        link.href = requirejs.toUrl("./main.css");
        console.log('css');
        console.log(link);
        document.getElementsByTagName("head")[0].appendChild(link);
    };

    // list tables
    function html_table(varList) {//jsonVars) {

        var table_dataTypes = ['DataFrame', 'ndarray', 'Series', 'list'];

        function _trunc(x, L) {
            console.log(x);
            x = String(x);

            if (x.length >= L) x = x.substring(0, L - 3) + '...';

            return x
                .replace(/ /g, '&nbsp;')
                .replace(/\n/g,'<br/>');
        }

        var kernelLanguage = Jupyter.notebook.metadata.kernelspec.language.toLowerCase()
        var kernel_config = cfg.kernels_config[kernelLanguage];

        var kernel_id = String(Jupyter.notebook.kernel.id);
        var shape_str = '';
        var has_shape = false;
        if (varList.some(listVar => "varShape" in listVar && listVar.varShape !== '')) { //if any of them have a shape
            shape_str = '<th class="sorter-false">Shape</th>';
            has_shape = true;
        }
        var beg_table = '<div class=\"inspector\"><table class=\"table fixed table-condensed table-nonfluid \"><col /> \
     <col  /><thead><tr><th >Name</th><th >Type</th>' + shape_str + '<th class="sorter-false">Value</th><th class="sorter-false">Search</th></tr></thead><tr><td> \
     </td></tr>';
        varList.forEach(listVar => {
            var shape_col_str = '</td><td><code>';
            if (has_shape) {
                shape_col_str = '</td><td>' + listVar.varShape + '</td><td><code>';
            }
            var dtype_index = table_dataTypes.indexOf(String(listVar.varType));
            if (dtype_index != -1){
            console.log(String(listVar.varType));
            beg_table +=
                '<tr><td  class="table-one">' + listVar.varName + '</td><td>' + _trunc(listVar.varType, cfg.cols.lenType) +
                //'</td><td>' + listVar.varSize +
                 shape_col_str + _trunc(listVar.varContent, cfg.cols.lenVar) +
                '</code></td><td><button class=\'button\' onClick = \"Jupyter.notebook.events.trigger(\'searchTable\', {var_name : \'' + String(listVar.varName) + '\', kid: \'' + kernel_id + '\', mode : 1 }) \" alt="Additional">&#10504;</button>' +
                '<button class=\'button\' onClick = \"Jupyter.notebook.events.trigger(\'searchTable\', {var_name : \'' + String(listVar.varName) + '\', kid: \'' + kernel_id + '\', mode:2}) \" alt="Linkable">&#10238;</button>' +
                '<button class=\'button\' onClick = \"Jupyter.notebook.events.trigger(\'searchTable\', {var_name : \'' + String(listVar.varName) + '\', kid: \'' + kernel_id + '\', mode:3}) \" alt="Semantically related">&approxeq;</button></td>' +
                '</tr>';
            }
        });
        var full_table = beg_table + '</table></div>';
        //console.log(full_table)
        return full_table;
        }

    /* From Travis Horn, https://travishorn.com/building-json2table-turn-json-into-an-html-table-a57cf642b84a */
    function json2table(json, classes) {
      var cols = Object.keys(json[0]);

      var headerRow = '';
      var bodyRows = '';

      classes = classes || '';

      function capitalizeFirstLetter(string) {
        return string.charAt(0).toUpperCase() + string.slice(1);
      }

      cols.map(function(col) {
        headerRow += '<th>' + capitalizeFirstLetter(col) + '</th>';
      });

      json.map(function(row) {
        bodyRows += '<tr>';

        cols.map(function(colName) {
          bodyRows += '<td>' + row[colName] + '</td>';
        })

        bodyRows += '</tr>';
      });

      return '<table class="' +
             classes +
             '"><thead><tr>' +
             headerRow +
             '</tr></thead><tbody>' +
             bodyRows +
             '</tbody></table>';
    }

    function getCookie(name) {
        var r = document.cookie.match("\\b" + name + "=([^;]*)\\b");
        return r ? r[1] : undefined;
    }

    // show returned tables
    function html_data_table(jsonVars, mode) {
        function _trunc(x, L) {
            x = String(x)
            if (x.length < L) return x
            else return x.substring(0, L - 3) + '...'
        }
        var kernelLanguage = Jupyter.notebook.metadata.kernelspec.language.toLowerCase()
        var kernel_config = cfg.kernels_config[kernelLanguage];
        var varList = JSON.parse(String(jsonVars))
        var kernel_id = String(Jupyter.notebook.kernel.id);
        var shape_str = '';
        var has_shape = false;
        if (varList.some(listVar => "varShape" in listVar && listVar.varShape !== '')) { //if any of them have a shape
            shape_str = '<th >Shape</th>';
            has_shape = true;
        }

        var caption = '';
        if (mode === 1){
            console.log('return result 1');
            //var beg_table = '<p><b>Similar Tables</b></p>';
            caption = 'Additional';
        }
        else if (mode === 2){
            console.log('return result 2');
            //var beg_table = '<p><b>Linkable Tables</b></p>';
            caption = 'Linkable';
        }
        else if (mode === 3){
            console.log('return result 3');
            //var beg_table = '<p><b>Role Similar Tables</b></p>';
            caption = 'Semantically Related';
        }

        var beg_table = '<div class=\"inspector\"><table class=\"table fixed table-condensed table-nonfluid \"><col /> \
     <thead><tr><th >Table</th>' + shape_str + '<th >' + caption + ' content</th></tr></thead><tr><td> \
     </td></tr>';
        var count = 0
        varList.forEach(listVar => {
            var shape_col_str = '</td><td>';
            if (has_shape) {
                shape_col_str = '</td><td>' + listVar.varShape + '</td><td>';
            }
            //var djson = '{\'varname\':\'' + listVar.varName + '\'}';
            //var jstr = listVar.varContent;
            count += 1;
            //var table_string = jstr.replace('\'', '\"');
            beg_table +=
                '<tr>' +//<td>' + String(count) + '</td>'
                '<td><button onClick = \"Jupyter.notebook.events.trigger(\'importtable\', {var_name : \'' + String(listVar.varName) + '\', kid: \'' + kernel_id + '\', mode:0}) \">' + listVar.varName + '</button></td>' +
                '<td>' + listVar.varContent +
                '</td>' + // <td><button onClick = \"Jupyter.notebook.events.trigger(\'importtable\', {var_name : \'' + String(listVar.varName) + '\', kid: \'' + kernel_id + '\', mode:0}) \">import</button></td>' +
                '</tr>';
        });
        var full_table = beg_table + '</table></div>';
        //console.log(full_table)
        return full_table;
        }


    function code_exec_callback(msg) {
        var jsonVars = msg.content['text'];
        var table_dataTypes = ['DataFrame', 'ndarray', 'Series'];

        var notWellDefined = false;
        if (msg.content.evalue) 
            notWellDefined = msg.content.evalue == "name 'var_dic_list' is not defined" || 
        msg.content.evalue.substr(0,28) == "Error in cat(var_dic_list())"
        //means that var_dic_list was cleared ==> need to retart the extension
        if (notWellDefined) dataset_inspector_init() 
        else {
            var varList = JSON.parse(String(jsonVars))

            // Turn this into a table
            $('#dataset_inspector').html(html_table(varList));

            // Call to index the new table!
            var kernel_id = String(Jupyter.notebook.kernel.id);
            varList.forEach(listVar => {
                var data = {'name': listVar.varName,
                            'id': kernel_id};
                console.log("here");
                console.log(listVar);
                var dtype_index = table_dataTypes.indexOf(String(listVar.varType));

                if(dtype_index != -1){
                    indexTable(data);
                }
            });
        }

        requirejs(['nbextensions/dataset_inspector/jquery.tablesorter.min'],
            function() {
        setTimeout(function() { if ($('#dataset_inspector').length>0)
            $('#dataset_inspector table').tablesorter()}, 50)
        });
    }

    function tableSort(name) {
        requirejs(['nbextensions/dataset_inspector/jquery.tablesorter.min'])
        $('#var' + name + ' table').tablesorter()
    }

    var varRefresh = function() {
        var kernelLanguage = Jupyter.notebook.metadata.kernelspec.language.toLowerCase()
        var kernel_config = cfg.kernels_config[kernelLanguage];
        requirejs(['nbextensions/dataset_inspector/jquery.tablesorter.min'],
            function() {
                Jupyter.notebook.kernel.execute(
                    kernel_config.varRefreshCmd, { iopub: { output: code_exec_callback } }, { silent: false }
                );
            });
    }

    /**
     * Index a table with a given name
     *
     * @param data
     */
    function indexTable(data) {
        var var_name = data.name;
        var kid = data.kid;

        console.log('Indexing ' + var_name);

        var send_url = utils.url_path_join(Jupyter.notebook.base_url, '/juneau');

        var return_data = ""
        var return_state = ""

        var cells = Jupyter.notebook.get_cells();
        var clen = Jupyter.notebook.get_selected_cells_indices()[0];
        var nb_name = Jupyter.notebook.notebook_path;
        var kernel_id = Jupyter.notebook.kernel.id;

        var i;
        var cell_code = "";
        var cell_id_count = 0;
        for (i = 0; i < clen; i++) {
            if(cells[i].cell_type === 'code'){
                cell_code = cell_code + cells[i].get_text() + '\n#\n';
                cell_id_count = cell_id_count + 1;
            }
        }
        console.log(cell_code);
        var data_json = {'var': var_name, 'code':cell_code, 'nb_name':nb_name, 'cell_id':cell_id_count, 'kid':kernel_id,
                            "_xsrf": getCookie("_xsrf")};

        $.ajax({
            url: send_url,
            type: 'PUT',
            data: data_json,
            dataType: 'json',
            timeout: 10000000,
            success : function (response) {
                return_state = response['state'];
                return_data = response['res'];
                if(return_state === 'true'){
                    var print_string = return_data.toString();
                }
                else{
                    alert("Error indexing table!");
                }
            },
            error: function (request, error) {
                console.log(arguments);
                alert("Can't index table because: " + error);
            }
        });
    }

    function searchTable(evt, data){
        console.log('Searching...');
        var mode = data.mode;
        var var_name = data.var_name;
        var kid = data.kid;

        var send_url = utils.url_path_join(Jupyter.notebook.base_url, '/juneau');

        var return_data = ""
        var return_state = ""

        var cells = Jupyter.notebook.get_cells()
        var clen = Jupyter.notebook.get_selected_cells_indices()[0]

        var i;
        var cell_code = "";
        for (i = 0; i < clen; i++) {
            if(cells[i].cell_type === 'code'){
                cell_code = cell_code + cells[i].get_text() + '\n';
            }
        }
        var data_json = {'var': var_name, 'kid':kid, 'mode': mode, 'code':cell_code,
                            "_xsrf": getCookie("_xsrf")};

        $.ajax({
            url: send_url,
            type: 'POST',
            data: data_json,
            dataType: 'json',
            timeout: 100000,
            success : function (response) {
                return_state = response['state'];
                return_data = response['res'];
                //console.log('Take A Look of Results.')
                //console.log(return_state)
                //console.log(return_data)
                if(return_state === 'true'){
                    var print_string = return_data.toString();
                    search_inspector(cfg, st, 'searchResults' + String(mode), mode);
                    $('#searchResults' + String(mode)).html(html_data_table(print_string, mode));
                    $('#searchResults' + String(mode) + '-wrapper').css('display', 'block');
                    //$('#' + name + '-wrapper').css('display', Jupyter.notebook.metadata['searchResults' + String(mode)]['window_display'] ? 'block' : 'block');
                } else if (response['error']) {
                    alert(response['error']);
                }
                else{
                    var print_string = 'print(\'No table returned!\')';
                    //console.log(print_string);
                    if(String(mode) == '1'){
                        alert("Sorry, no similar table detected!");
                    }
                    else if (String(mode) == '2'){
                        alert("Sorry, no linkable table detected!");
                    }
                    else{
                        alert("Sorry no table detected!");
                    }

                }
            },
            error: function (request, error) {
                console.log(arguments);
                alert(" Can't do because: " + error);
            }
        });
    }

//    function importtable(evt, data){
//        var var_name = data.var_name;
//        var kid = data.kid;
//        var mode = data.mode;
//        var data_json = {'var': var_name, 'kid':kid, 'mode': mode};
//
//        var send_url = utils.url_path_join(Jupyter.notebook.base_url, '/juneau');
//        var return_data = ""
//        var return_state = ""
//
//        $.ajax({
//            url: send_url,
//            type: 'GET',
//            data: data_json,
//            dataType: 'json',
//            success : function (response) {
//                return_state = response['state'];
//                return_data = response['res'];
//                if(return_state === 'true'){
//                    var print_string = return_data.toString();
//                    var cell = Jupyter.notebook.insert_cell_below('code');
//                    cell.set_text("#Import New Table");
//                    cell.execute();
//                    var cell_id = Jupyter.notebook.get_selected_cells_indices()[0] + 1;
//                    var rcell = Jupyter.notebook.insert_cell_below('code', cell_id);
//
//                    rcell.set_text('eng = juneau_connect()\n' + var_name + '_df = pd.read_sql_table(\'' + var_name + '\', eng)\n' + var_name + '_df');
//                    rcell.execute();
//                }
//                else{
//                    var print_string = 'print(\'the search table is not in this cell!\')';
//                    console.log(print_string);
//                }
//            },
//            error : utils.log_ajax_error
//        });
//    }

    function importtable(evt, data){
        var var_name = data.var_name;
        var kid = data.kid;
        var mode = data.mode;
        var data_json = {'var': var_name, 'kid':kid, 'mode': mode};

        var cell = Jupyter.notebook.insert_cell_below('code');
        cell.set_text("#Import New Table\neng = juneau_connect()\nnew_data_df = pd.read_sql_table(\'" + var_name + "\', eng)\nprint(new_data_df.head())");
        cell.execute();
        //var cell_id = Jupyter.notebook.get_selected_cells_indices()[0] + 1;
        //var rcell = Jupyter.notebook.insert_cell_below('code', cell_id);

        //rcell.set_text();
        //rcell.execute();
    }

    var dataset_inspector_init = function() {
        // Define code_init
        // read and execute code_init 
        function read_code_init(lib) {
            var libName = Jupyter.notebook.base_url + "nbextensions/dataset_inspector/" + lib;
            $.get(libName).done(function(data) {
                st.code_init = data;
                st.code_init = st.code_init.replace('lenName', cfg.cols.lenName).replace('lenType', cfg.cols.lenType)
                        .replace('lenVar', cfg.cols.lenVar)
                        //.replace('types_to_exclude', JSON.stringify(cfg.types_to_exclude).replace(/\"/g, "'"))
                requirejs(
                        [
                            'nbextensions/dataset_inspector/jquery.tablesorter.min'
                            //'nbextensions/dataset_inspector/colResizable-1.6.min'
                        ],
                        function() {
                            Jupyter.notebook.kernel.execute(st.code_init, { iopub: { output: code_exec_callback } }, { silent: false });
                        })
                    variable_inspector(cfg, st);  // create window if not already present      
                console.log(log_prefix + 'loaded library');
            }).fail(function() {
                console.log(log_prefix + 'failed to load ' + lib + ' library')
            });
        }

            // read configuration  

            cfg = read_config('dataset_inspector',cfg, function() {
            // Called when config is available
                if (typeof Jupyter.notebook.kernel !== "undefined" && Jupyter.notebook.kernel !== null) {
                    var kernelLanguage = Jupyter.notebook.metadata.kernelspec.language.toLowerCase()
                    var kernel_config = cfg.kernels_config[kernelLanguage];
                    if (kernel_config === undefined) { // Kernel is not supported
                        console.warn(log_prefix + " Sorry, can't use kernel language " + kernelLanguage + ".\n" +
                            "Configurations are currently only defined for the following languages:\n" +
                            Object.keys(cfg.kernels_config).join(', ') + "\n" +
                            "See readme for more details.");
                        if ($("#dataset_inspector_button").length > 0) { // extension was present
                            $("#dataset_inspector_button").remove(); 
                            $('#dataset_inspector-wrapper').remove();
                            // turn off events
                            events.off('execute.CodeCell', varRefresh); 
                            events.off('varRefresh', varRefresh);
                            events.off('searchTable', searchTable);
                        }
                        return
                    }
                    dataset_inspector_button(); // In case button was removed 
                    // read and execute code_init (if kernel is supported)
                    read_code_init(kernel_config.library);
                    // console.log("code_init-->", st.code_init)
                    } else {
                    console.warn(log_prefix + "Kernel not available?");
                    }
            }); // called after config is stable  

            // event: on cell execution, update the list of variables 
            events.on('execute.CodeCell', varRefresh);
            events.on('varRefresh', varRefresh);
            events.on('searchTable', searchTable);
            events.on('importtable', importtable);
            events.on('toggleSearch', toggleSearch);
            }

    var create_search_div = function(cfg, st, name, title) {
        create_named_div(name, title, cfg, st);
    }

    var create_dataset_inspector_div = function(cfg, st) {
        create_named_div('dataset_inspector', 'Notebook Datasets', cfg, st);
    }

    var create_named_div = function(name, title, cfg, st) {

        function save_position(){
            if (!Jupyter.notebook.metadata[name])
                Jupyter.notebook.metadata[name] = {};

            Jupyter.notebook.metadata[name].position = {
                'left': $('#' + name + '-wrapper').css('left'),
                'top': $('#' + name + '-wrapper').css('top'),
                'width': $('#' + name + '-wrapper').css('width'),
                'height': $('#' + name + '-wrapper').css('height'),
                'right': $('#' + name + '-wrapper').css('right')
            };
        }
        var dataset_inspector_wrapper = $('<div id="' + name + '-wrapper"/>')
            .append(
                $('<div id="' + name + '-header"/>')
                .addClass("header")
                .text(title + " ")
                .append(
                    $("<a/>")
                    .attr("href", "#")
                    .text("x")
                    .addClass("button kill-btn")
                    .attr('title', 'Close window')
                    .click(function() {
                        if(name === 'dataset_inspector'){
                            toggleVarInspector();
                        }
                        else{
                            var json_string = {vname : name};
                            toggleSearch(json_string);
                        }

                        return false;
                    })
                )
                .append(
                    $("<a/>")
                    .attr("href", "#")
                    .addClass("button hide-btn")
                    .attr('title', 'Hide ' + title)
                    .text("-")
                    .click(function() {
                        $('#' + name + '-wrapper').css('position', 'fixed');
                        $('#' + name).slideToggle({
                            start: function(event, ui) {
                                // $(this).width($(this).width());
                            },
                            'complete': function() {
                                    Jupyter.notebook.metadata[name][name + '_section_display'] = $('#' + name).css('display');
                                    save_position();
                                    Jupyter.notebook.set_dirty();
                            }
                        });
                        $('#' + name + '-wrapper').toggleClass('closed');
                        if ($('#' + name + '-wrapper').hasClass('closed')) {
                            cfg.oldHeight = $('#' + name + '-wrapper').height(); //.css('height');
                            $('#' + name + '-wrapper').css({ height: 40 });
                            $('#' + name + '-wrapper .hide-btn')
                                .text('+')
                                .attr('title', 'Show ' + title);
                        } else {
                            $('#' + name + '-wrapper').height(cfg.oldHeight); //css({ height: cfg.oldHeight });
                            $('#' + name).height(cfg.oldHeight - $('#' + name + '-header').height() - 30 )
                            $('#' + name + '-wrapper .hide-btn')
                                .text('-')
                                .attr('title', 'Hide ' + title);
                        }
                        return false;
                    })
                ).append(
                    $("<a/>")
                    .attr("href", "#")
                    .text("  \u21BB")
                    .addClass("button reload-btn")
                    .attr('title', 'Refresh Juneau')
                    .click(function() {
                        //variable_inspector(cfg,st); 
                        varRefresh();
                        return false;
                    })
                ).append(
                    $("<span/>")
                    .html("&nbsp;&nbsp")
                ).append(
                    $("<span/>")
                    .html("&nbsp;&nbsp;")
                )
            ).append(
                $("<div/>").attr("id", name).addClass(name)
            )

        $("body").append(dataset_inspector_wrapper);
        // Ensure position is fixed
        $('#' + name + '-wrapper').css('position', 'fixed');

        // enable dragging and save position on stop moving
        $('#' + name + '-wrapper').draggable({
            drag: function(event, ui) {}, //end of drag function
            start: function(event, ui) {
                $(this).width($(this).width());
            },
            stop: function(event, ui) { // on save, store window position
                    save_position();
                    Jupyter.notebook.set_dirty();
                // Ensure position is fixed (again)
                $('#' + name + '-wrapper').css('position', 'fixed');
            },
        });

        $('#' + name + '-wrapper').resizable({
            resize: function(event, ui) {
                $('#' + name).height($('#' + name + '-wrapper').height() - $('#' + name + '-header').height());
            },
            start: function(event, ui) {
                //$(this).width($(this).width());
                $(this).css('position', 'fixed');
            },
            stop: function(event, ui) { // on save, store window position
                    save_position();
                    $('#' + name).height($('#' + name + '-wrapper').height() - $('#' + name + '-header').height())
                    Jupyter.notebook.set_dirty();
                // Ensure position is fixed (again)
                //$(this).css('position', 'fixed');
            }
        })

        // restore window position at startup
        //console.log(name);
        //console.log(Jupyter.notebook.metadata);
            if (Jupyter.notebook.metadata[name] && Jupyter.notebook.metadata[name].position) {
                $('#' + name + '-wrapper').css(Jupyter.notebook.metadata[name].position);
            }
        // Ensure position is fixed
        $('#' + name + '-wrapper').css('position', 'fixed');

        // Restore window display 
            if (Jupyter.notebook.metadata[name]) {
                if (Jupyter.notebook.metadata[name][name + '_section_display']) {
                    $('#' + name).css('display', Jupyter.notebook.metadata[name][name + '_section_display'])
                    //$('#dataset_inspector').css('height', $('#dataset_inspector-wrapper').height() - $('#dataset_inspector-header').height())
                    if (Jupyter.notebook.metadata[name][name + '_section_display'] == 'none') {
                        $('#' + name + '-wrapper').addClass('closed');
                        $('#' + name + '-wrapper').css({ height: 40 });
                        $('#' + name + '-wrapper .hide-btn')
                            .text('+')
                            .attr('title', 'Show ' + title);
                    }
                }
                if (Jupyter.notebook.metadata[name]['window_display']) {
                    console.log(log_prefix + "Restoring " + name + " window");
                    console.log(Jupyter.notebook.metadata[name]['window_display']);
                    $('#' + name + '-wrapper').css('display', 'block');

                    if ($('#' + name + '-wrapper').hasClass('closed')){
                        $('#' + name).height(cfg.oldHeight - $('#' + name + '-header').height())
                    }else{
                        $('#' + name).height($('#' + name + '-wrapper').height() - $('#' + name + '-header').height()-30)
                    }
                    
                }
            }
        // if dataset_inspector-wrapper is undefined (first run(?), then hide it)
        if (!($('#' + name + '-wrapper').css('display')))
            $('#' + name + '-wrapper').css('display', "none") //block

        dataset_inspector_wrapper.addClass(name + '-float-wrapper');
    }

    var variable_inspector = function(cfg, st) {

        var dataset_inspector_wrapper = $("#dataset_inspector-wrapper");
        if (dataset_inspector_wrapper.length === 0) {
            create_dataset_inspector_div(cfg, st);
        }

        $(window).resize(function() {
            $('#dataset_inspector').css({ maxHeight: $(window).height() - 30 });
            $('#dataset_inspector-wrapper').css({ maxHeight: $(window).height() - 10 });
        });

        $(window).trigger('resize');
        varRefresh();
    };

    var search_inspector = function(cfg, st, name, mode) {

        var dataset_inspector_wrapper = $("#" + name + "-wrapper");

        var caption = '';
        if (mode === 1){
            caption = 'Similar Datasets';
        }
        else if (mode === 2){
            caption = 'Linkable Datasets';
        }
        else if (mode === 3){
            caption = 'Related Datasets';
        }

        if (dataset_inspector_wrapper.length === 0) {
            create_search_div(cfg, st, name, caption);
        }

        $(window).resize(function() {
            $('#' + name ).css({ maxHeight: $(window).height() - 30 });
            $('#' + name + '-wrapper').css({ maxHeight: $(window).height() - 10 });
        });

        $(window).trigger('resize');
    };

    var toggle_dataset_inspector = function(cfg, st) {
        // toggle draw (first because of first-click behavior)

        $("#dataset_inspector-wrapper").toggle({
            'progress': function() {},
            'complete': function() {
                    Jupyter.notebook.metadata['dataset_inspector']['window_display'] = $('#dataset_inspector-wrapper').css('display') == 'block';
                    Jupyter.notebook.set_dirty();
                // recompute:
                variable_inspector(cfg, st);
            }
        });
    };

    var toggle_search = function(name, cfg, st) {
        // toggle draw (first because of first-click behavior)

        $("#" + name + "-wrapper").toggle({
            'progress': function() {},
            'complete': function() {
                    Jupyter.notebook.metadata[name]['window_display'] = $('#' + name + '-wrapper').css('display') == 'block';
                    Jupyter.notebook.set_dirty();
                search_inspector(cfg, st, name);
            }
        });
    };

    var load_jupyter_extension = function() {
        load_css(); //console.log("Loading css")
        dataset_inspector_button(); //console.log("Adding dataset_inspector_button")

        // If a kernel is available, 
        if (typeof Jupyter.notebook.kernel !== "undefined" && Jupyter.notebook.kernel !== null) {
            console.log(log_prefix + "Kernel is available -- dataset_inspector initializing ")
            dataset_inspector_init();
        }
        // if a kernel wasn't available, we still wait for one. Anyway, we will run this for new kernel 
        // (test if is is a Python kernel and initialize)
        // on kernel_ready.Kernel, a new kernel has been started and we shall initialize the extension
        events.on("kernel_ready.Kernel", function(evt, data) {
            console.log(log_prefix + "Kernel is available -- reading configuration");
            dataset_inspector_init();
        });
    };

    return {
        load_ipython_extension: load_jupyter_extension,
        varRefresh: varRefresh
    };

});
