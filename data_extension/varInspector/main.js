define([
    'require',
    'jquery',
    'base/js/namespace',
    'base/js/events',
    'notebook/js/codecell',
    'notebook/js/notebook',
    'base/js/utils',
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

    var mod_name = "varInspector";
    var log_prefix = '[' + mod_name + '] ';


    // ...........Parameters configuration......................
    // define default values for config parameters if they were not present in general settings (notebook.json)
    var cfg = {
        'window_display': false,
        'cols': {
            'lenName': 16,
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
        cell.set_text("#Tabled Imported");

    };

    //.....................global variables....


    var st = {}
    st.config_loaded = false;
    st.extension_initialized = false;
    st.code_init = "";

    function read_config(name, cfg, callback) { // read after nb is loaded
        console.log(name);
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
        toggle_varInspector(cfg, st)
    }

    function toggleSearch(data){
        console.log(data);
        toggle_search(data.vname, cfg, st)
    }
    var varInspector_button = function() {
        if (!Jupyter.toolbar) {
            events.on("app_initialized.NotebookApp", varInspector_button);
            return;
        }
        if ($("#varInspector_button").length === 0) {
            $(Jupyter.toolbar.add_buttons_group([
                Jupyter.keyboard_manager.actions.register ({
                    'help'   : 'Variable Inspector',
                    'icon'   : 'fa-crosshairs',
                    'handler': toggleVarInspector,
                }, 'toggle-variable-inspector', 'varInspector')
            ])).find('.btn').attr('id', 'varInspector_button');
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


function html_table(jsonVars) {
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
    var beg_table = '<div class=\"inspector\"><table class=\"table fixed table-condensed table-nonfluid \"><col /> \
 <col  /><col /><thead><tr><th >Name</th><th >Type</th><th >Size</th>' + shape_str + '<th >Value</th><th>Search</th></tr></thead><tr><td> \
 </td></tr>';
    varList.forEach(listVar => {
        var shape_col_str = '</td><td>';
        if (has_shape) {
            shape_col_str = '</td><td>' + listVar.varShape + '</td><td>';
        }
        //var djson = '{\'varname\':\'' + listVar.varName + '\'}';
        //var jstr = listVar.varContent;
        beg_table +=
            '<tr><td>' + _trunc(listVar.varName, cfg.cols.lenName) + '</td><td>' + _trunc(listVar.varType, cfg.cols.lenType) +
            '</td><td>' + listVar.varSize + shape_col_str + _trunc(listVar.varContent, cfg.cols.lenVar) +
            '</td><td><button onClick = \"Jupyter.notebook.events.trigger(\'searchTable\', {var_name : \'' + String(listVar.varName) + '\', kid: \'' + kernel_id + '\', mode : 1 }) \">s</button>' +
            '<button onClick = \"Jupyter.notebook.events.trigger(\'searchTable\', {var_name : \'' + String(listVar.varName) + '\', kid: \'' + kernel_id + '\', mode:2}) \">l</button>' +
            '<button onClick = \"Jupyter.notebook.events.trigger(\'searchTable\', {var_name : \'' + String(listVar.varName) + '\', kid: \'' + kernel_id + '\', mode:3}) \">r</button></td>' +
            '</tr>';
    });
    var full_table = beg_table + '</table></div>';
    return full_table;
    }

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

    if (mode === 1){
        //console.log('return result 1');
        var beg_table = '<p><b>Similar Tables</b></p>';
    }
    else if (mode === 2){
        //console.log('return result 2');
        var beg_table = '<p><b>Linkable Tables</b></p>';
    }
    else{
        //console.log('return result 3');
        var beg_table = '<p><b>Role Similar Tables</b></p>';
    }

    beg_table += '<div class=\"inspector\"><table class=\"table fixed table-condensed table-nonfluid \"><col /> \
 <col  /><col /><thead><tr><th>Rank</th><th >Name</th>' + shape_str + '<th >Value</th><th>Operation</th></tr></thead><tr><td> \
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
            '<tr><td>' + String(count) + '</td><td>' + _trunc(listVar.varName, cfg.cols.lenName) + '</td>' +
            '<td>' + listVar.varContent +
            '</td> <td><button onClick = \"Jupyter.notebook.events.trigger(\'importtable\', {var_name : \'' + String(listVar.varName) + '\', kid: \'' + kernel_id + '\', mode:0}) \">import</button></td>' +
            '</tr>';
    });
    var full_table = beg_table + '</table></div>';
    return full_table;
    }


    function code_exec_callback(msg) {
        var jsonVars = msg.content['text'];
        var notWellDefined = false;
        if (msg.content.evalue) 
            notWellDefined = msg.content.evalue == "name 'var_dic_list' is not defined" || 
        msg.content.evalue.substr(0,28) == "Error in cat(var_dic_list())"
        //means that var_dic_list was cleared ==> need to retart the extension
        if (notWellDefined) varInspector_init() 
        else $('#varInspector').html(html_table(jsonVars))
        
        requirejs(['nbextensions/varInspector/jquery.tablesorter.min'],
            function() {
        setTimeout(function() { if ($('#varInspector').length>0)
            $('#varInspector table').tablesorter()}, 50)
        });
    }

    function tableSort(name) {
        requirejs(['nbextensions/varInspector/jquery.tablesorter.min'])
        $('#var' + name + ' table').tablesorter()
    }

    var varRefresh = function() {
        var kernelLanguage = Jupyter.notebook.metadata.kernelspec.language.toLowerCase()
        var kernel_config = cfg.kernels_config[kernelLanguage];
        requirejs(['nbextensions/varInspector/jquery.tablesorter.min'],
            function() {
                Jupyter.notebook.kernel.execute(
                    kernel_config.varRefreshCmd, { iopub: { output: code_exec_callback } }, { silent: false }
                );
            });
    }

    function searchTable(evt, data){
        var mode = data.mode;
        var var_value = data.var_name;
        var kid = data.kid;
        var data_json = {'var': var_value, 'kid':kid, 'mode': mode};
        var send_url = utils.url_path_join(Jupyter.notebook.base_url, '/stable');

        var return_data = ""
        var return_state = ""
        console.log('here to search!');

        $.ajax({
            url: send_url,
            type: 'GET',
            data: data_json,
            dataType: 'json',
            timeout: 100000,
            success : function (response) {
                return_state = response['state'];
                return_data = response['res'];
                if(return_state === 'true'){
                    var print_string = return_data.toString();
                    search_inspector(cfg, st, String(mode));

                    //requirejs(['nbextensions/varInspector/jquery.tablesorter.min'],
                    //    function() {
                    //setTimeout(function() { if ($('#searchResults' + String(mode)).length>0)
                    //    $('#searchResults' + String(mode) + ' table').tablesorter()}, 50)
                    //});
                    //console.log('#searchResults' + String(mode));
                    //console.log(print_string);
                    //console.log(mode);
                    $('#searchResults' + String(mode)).html(html_data_table(print_string, mode));
                }
                else{
                    var print_string = 'print(\'No table returned!\')';
                    console.log(print_string);
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

    function importtable(evt, data){
        var var_name = data.var_name;
        var kid = data.kid;
        var mode = data.mode;
        var data_json = {'var': var_name, 'kid':kid, 'mode': mode};

        var send_url = utils.url_path_join(Jupyter.notebook.base_url, '/stable');
        var return_data = ""
        var return_state = ""

        $.ajax({
            url: send_url,
            type: 'GET',
            data: data_json,
            dataType: 'json',
            success : function (response) {
                return_state = response['state'];
                return_data = response['res'];
                if(return_state === 'true'){
                    var print_string = return_data.toString();

                    var cell = Jupyter.notebook.insert_cell_below('code');
                    cell.set_text("#Import New Table");
                    cell.execute();
                    var cell_id = Jupyter.notebook.get_selected_cells_indices()[0] + 1;
                    var rcell = Jupyter.notebook.insert_cell_below('code', cell_id);
                    var running_code = 'from sqlalchemy import create_engine\nuser_name = \'yizhang\'\npassword = \'yizhang\'\ndbname = \'joinstore\'\n' +
                        'def connect2db():\n\tengine = create_engine(\'postgresql://\' + user_name + \':\' + password + \'@localhost/\' + dbname)\n\treturn engine.connect()';
                    execute_code(running_code, rcell);
                    rcell.set_text('eng = connect2db()\ndf_new = pd.read_sql_table(\'' + var_name + '\', eng)\nprint(df_new)');
                }
                else{
                    var print_string = 'print(\'the search table is not in this cell!\')';
                    console.log(print_string);
                }
            },
            error : utils.log_ajax_error
        });
    }

    var varInspector_init = function() {
        // Define code_init
        // read and execute code_init 
        function read_code_init(lib) {
            var libName = Jupyter.notebook.base_url + "nbextensions/varInspector/" + lib;
            $.get(libName).done(function(data) {
                st.code_init = data;
                st.code_init = st.code_init.replace('lenName', cfg.cols.lenName).replace('lenType', cfg.cols.lenType)
                        .replace('lenVar', cfg.cols.lenVar)
                        //.replace('types_to_exclude', JSON.stringify(cfg.types_to_exclude).replace(/\"/g, "'"))
                requirejs(
                        [
                            'nbextensions/varInspector/jquery.tablesorter.min'
                            //'nbextensions/varInspector/colResizable-1.6.min'
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

            cfg = read_config('varInspector',cfg, function() {
            // Called when config is available
                if (typeof Jupyter.notebook.kernel !== "undefined" && Jupyter.notebook.kernel !== null) {
                    var kernelLanguage = Jupyter.notebook.metadata.kernelspec.language.toLowerCase()
                    var kernel_config = cfg.kernels_config[kernelLanguage];
                    if (kernel_config === undefined) { // Kernel is not supported
                        console.warn(log_prefix + " Sorry, can't use kernel language " + kernelLanguage + ".\n" +
                            "Configurations are currently only defined for the following languages:\n" +
                            Object.keys(cfg.kernels_config).join(', ') + "\n" +
                            "See readme for more details.");
                        if ($("#varInspector_button").length > 0) { // extension was present
                            $("#varInspector_button").remove(); 
                            $('#varInspector-wrapper').remove();
                            // turn off events
                            events.off('execute.CodeCell', varRefresh); 
                            events.off('varRefresh', varRefresh);
                            events.off('searchTable', searchTable);
                        }
                        return
                    }
                    varInspector_button(); // In case button was removed 
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

    var create_search_div = function(cfg, st, mode) {
        create_named_div('searchResults' + String(mode), 'Search Results', cfg, st);
    }

    var create_varInspector_div = function(cfg, st) {
        create_named_div('varInspector', 'List All Tables (DataFrame/Array)', cfg, st);
    }

    var create_named_div = function(name, title, cfg, st) {

        function save_position(){
            Jupyter.notebook.metadata[name].position = {
                'left': $('#' + name + '-wrapper').css('left'),
                'top': $('#' + name + '-wrapper').css('top'),
                'width': $('#' + name + '-wrapper').css('width'),
                'height': $('#' + name + '-wrapper').css('height'),
                'right': $('#' + name + '-wrapper').css('right')
            };
        }
        var varInspector_wrapper = $('<div id="' + name + '-wrapper"/>')
            .append(
                $('<div id="' + name + '-header"/>')
                .addClass("header")
                .text(title + " ")
                .append(
                    $("<a/>")
                    .attr("href", "#")
                    .text("[x]")
                    .addClass("kill-btn")
                    .attr('title', 'Close window')
                    .click(function() {
                        if(name === 'varInspector'){
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
                    .addClass("hide-btn")
                    .attr('title', 'Hide ' + title)
                    .text("[-]")
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
                                .text('[+]')
                                .attr('title', 'Show ' + title);
                        } else {
                            $('#' + name + '-wrapper').height(cfg.oldHeight); //css({ height: cfg.oldHeight });
                            $('#' + name).height(cfg.oldHeight - $('#' + name + '-header').height() - 30 )
                            $('#' + name + '-wrapper .hide-btn')
                                .text('[-]')
                                .attr('title', 'Hide ' + title);
                        }
                        return false;
                    })
                ).append(
                    $("<a/>")
                    .attr("href", "#")
                    .text("  \u21BB")
                    .addClass("reload-btn")
                    .attr('title', 'Reload ' + title)
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

        $("body").append(varInspector_wrapper);
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
        console.log(name);
        console.log(Jupyter.notebook.metadata);
            if (Jupyter.notebook.metadata[name].position !== undefined) {
                $('#' + name + '-wrapper').css(Jupyter.notebook.metadata[name].position);
            }
        // Ensure position is fixed
        $('#' + name + '-wrapper').css('position', 'fixed');

        // Restore window display 
            if (Jupyter.notebook.metadata[name] !== undefined) {
                if (Jupyter.notebook.metadata[name][name + '_section_display'] !== undefined) {
                    $('#' + name).css('display', Jupyter.notebook.metadata[name][name + '_section_display'])
                    //$('#varInspector').css('height', $('#varInspector-wrapper').height() - $('#varInspector-header').height())
                    if (Jupyter.notebook.metadata[name][name + '_section_display'] == 'none') {
                        $('#' + name + '-wrapper').addClass('closed');
                        $('#' + name + '-wrapper').css({ height: 40 });
                        $('#' + name + '-wrapper .hide-btn')
                            .text('[+]')
                            .attr('title', 'Show ' + title);
                    }
                }
                if (Jupyter.notebook.metadata[name]['window_display'] !== undefined) {
                    console.log(log_prefix + "Restoring Variable Inspector window");
                    $('#' + name + '-wrapper').css('display', Jupyter.notebook.metadata[name]['window_display'] ? 'block' : 'none');
                    if ($('#' + name + '-wrapper').hasClass('closed')){
                        $('#' + name).height(cfg.oldHeight - $('#' + name + '-header').height())
                    }else{
                        $('#' + name).height($('#' + name + '-wrapper').height() - $('#' + name + '-header').height()-30)
                    }
                    
                }
            }
        // if varInspector-wrapper is undefined (first run(?), then hide it)
        if ($('#' + name + '-wrapper').css('display') == undefined) $('#' + name + '-wrapper').css('display', "none") //block

        varInspector_wrapper.addClass(name + '-float-wrapper');
    }

    var variable_inspector = function(cfg, st) {

        var varInspector_wrapper = $("#varInspector-wrapper");
        if (varInspector_wrapper.length === 0) {
            create_varInspector_div(cfg, st);
        }

        $(window).resize(function() {
            $('#varInspector').css({ maxHeight: $(window).height() - 30 });
            $('#varInspector-wrapper').css({ maxHeight: $(window).height() - 10 });
        });

        $(window).trigger('resize');
        varRefresh();
    };

    var search_inspector = function(cfg, st, mode) {

        cfg = read_config('searchResults' + String(mode), cfg, function() {
            // Called when config is available
            if (typeof Jupyter.notebook.kernel !== "undefined" && Jupyter.notebook.kernel !== null) {
                var kernelLanguage = Jupyter.notebook.metadata.kernelspec.language.toLowerCase()
                var kernel_config = cfg.kernels_config[kernelLanguage];
                if (kernel_config === undefined) { // Kernel is not supported
                    console.warn(log_prefix + " Sorry, can't use kernel language " + kernelLanguage + ".\n" +
                        "Configurations are currently only defined for the following languages:\n" +
                        Object.keys(cfg.kernels_config).join(', ') + "\n" +
                        "See readme for more details.");
                    return
                }
            }
            else{
                console.warn(log_prefix + "Kernel not available?");
            }
        }); // called after config is stable

        var varInspector_wrapper = $("#searchResults" + String(mode) + "-wrapper");

        if (varInspector_wrapper.length === 0) {
            create_search_div(cfg, st, mode);
        }

        $(window).resize(function() {
            $('#searchResults' + String(mode)).css({ maxHeight: $(window).height() - 30 });
            $('#searchResults' + String(mode) + '-wrapper').css({ maxHeight: $(window).height() - 10 });
        });

        $(window).trigger('resize');
    };

    var toggle_varInspector = function(cfg, st) {
        // toggle draw (first because of first-click behavior)

        $("#varInspector-wrapper").toggle({
            'progress': function() {},
            'complete': function() {
                    Jupyter.notebook.metadata['varInspector']['window_display'] = $('#varInspector-wrapper').css('display') == 'block';
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
            }
        });
    };

    var load_jupyter_extension = function() {
        load_css(); //console.log("Loading css")
        varInspector_button(); //console.log("Adding varInspector_button")

        // If a kernel is available, 
        if (typeof Jupyter.notebook.kernel !== "undefined" && Jupyter.notebook.kernel !== null) {
            console.log(log_prefix + "Kernel is available -- varInspector initializing ")
            varInspector_init();
        }
        // if a kernel wasn't available, we still wait for one. Anyway, we will run this for new kernel 
        // (test if is is a Python kernel and initialize)
        // on kernel_ready.Kernel, a new kernel has been started and we shall initialize the extension
        events.on("kernel_ready.Kernel", function(evt, data) {
            console.log(log_prefix + "Kernel is available -- reading configuration");
            varInspector_init();
        });
    };

    return {
        load_ipython_extension: load_jupyter_extension,
        varRefresh: varRefresh
    };

});
