// file my_extension/main.js

define([
    'require',
    'jqueryui',
    'base/js/namespace',
    'base/js/utils',
    'services/config'
], function (
    requirejs,
    $,
    Jupyter,
    utils
) {
    'use strict';

    function filterRows (filterText) {
        var input = $('#variablekeyword');
        filterText = filterText !== undefined ? filterText : input.val();
        return filterText;
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
    };

    function filterRowsDefaultParams () {
        var text = filterRows();
        var nb = Jupyter.notebook;
        var cell = nb.get_selected_cell();
        console.log(cell.get_text());
        console.log(cell.get_text().trim().length);
        if (cell.get_text().trim().length === 0){
            execute_code('print(\''.concat(text).concat(' is not in this cell!\')'), cell);
            return;
        }
        console.log(cell.kernel);
        var kid = cell.kernel.id;
        var data_json = {'var':text, 'kid':kid};
        var send_url = utils.url_path_join(Jupyter.notebook.base_url, '/queryvariable');

        console.log(send_url);
        console.log(data_json);
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
                console.log(return_data);
                console.log(return_state);
                if(return_state === 'true'){
                    var print_string = return_data.toString()
                    print_string = print_string.substring(0, print_string.length-1);
                    var code = 'print(\''.concat(print_string).concat('\')');
                    console.log(code);
                    execute_code(code, cell);
                }
                else{
                    var print_string = 'print(\'the search variable is not in this cell!\')';
                    console.log(print_string);
                    execute_code(print_string, cell);
                }
            },
            error : utils.log_ajax_error
        });


    }


    function load_ipython_extension() {
        var form_tgrp = $('<div/>')
            .addClass('btn-group')                                                                                 // insert a top form-group to make the form appear next to the buttons
            .appendTo('#maintoolbar-container');

        var frm_grp = $('<div/>')
            .addClass('form-group')                                                                                 // insert a form-group
            .css('margin-bottom', 0)
            .appendTo(form_tgrp);

        var grp = $('<div/>')
            .addClass('input-group')                                                                                // insert an input-group
            .appendTo(frm_grp);

        $('<input/>')                                                                                               // insert search bar
            .attr('type', 'text')
            .addClass('form-control input-sm')
            .attr('title', 'variable for searching related tables')
            .attr('id', 'variablekeyword')
            .attr('placeholder', 'Variable to Search')
            .css('font-weight', 'bold')
            .css('width', '70%')
            .css('height', '24px')
            .on('focus', function (evt) { Jupyter.notebook.keyboard_manager.disable();})
            .on('blur', function (evt) { Jupyter.notebook.keyboard_manager.enable();})
            .appendTo(grp);

       $('<button/>')
            .attr('type', 'button')                                                                                 // insert regex button
            .attr('id', 'filterisreg')
            .addClass('btn btn-default btn-sm')
            .attr('data-toggle', 'button')
            .css('font-weight', 'bold')
            .attr('title', 'Use regex (JavaScript regex syntax)')
            .text('Search')
            .on('click', function (evt) { setTimeout(filterRowsDefaultParams); })
            .appendTo(grp);

    }

    return {
        load_ipython_extension: load_ipython_extension
    };
});