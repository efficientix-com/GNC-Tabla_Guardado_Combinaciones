/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @name FB_borrado_guardado_combinaciones_mr
 * @version 1.0
 * @author Dylan Mendoza <dylan.mendoza@freebug.mx>
 * @summary Script para el borrado de la tabla de Artus para continuar con la creación de la misma.
 * @copyright Tekiio México 2023
 * 
 * Client              -> GNC
 * Last modification   -> 10/07/2023
 * Modified by         -> Dylan Mendoza <dylan.mendoza@freebug.mx>
 * Script in NS        -> FB - Borrado de Combinaciones <customscript_fb_borrado_combinaciones>
 */
define(['N/log', 'N/record', 'N/search', 'N/task', 'N/runtime'],
    /**
 * @param{log} log
 * @param{record} record
 * @param{search} search
 * @param{task} task
 */
    (log, record, search, task, runtime) => {
        /**
         * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
         * @param {Object} inputContext
         * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Object} inputContext.ObjectRef - Object that references the input data
         * @typedef {Object} ObjectRef
         * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
         * @property {string} ObjectRef.type - Type of the record instance that contains the input data
         * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
         * @since 2015.2
         */

        const getInputData = (inputContext) => {
            try {
                var parameter_search = runtime.getCurrentScript().getParameter({name: "custscript_fb_borrado_search"});
                var parameter_type = runtime.getCurrentScript().getParameter({name: "custscript_fb_borrado_type"});
                var parameter_deploy = runtime.getCurrentScript().getParameter({name: "custscript_fb_borrado_deplo"});
                log.audit({title:'Params', details:{search: parameter_search, type: parameter_type, deploy: parameter_deploy}});
                var searchObj = search.load({
                    id: parameter_search
                });
                log.audit({title:'searchObj', details:searchObj});
                var dataEjecute = record.load({
                    type: record.Type.SCRIPT_DEPLOYMENT,
                    id: parameter_deploy
                });
                var continueProcess = dataEjecute.getValue({fieldId: 'custscript_tkio_fin_ciclo'});
                log.debug({title:'¿Se encuentra procesando?', details:continueProcess});
                if (searchObj.searchType == parameter_type && continueProcess == false) {
                    log.audit({title:'Inicio del borrado', details:'Inicio del borrado'});
                    dataEjecute.setValue({
                        fieldId: 'custscript_tkio_fin_ciclo',
                        value: true
                    });
                    dataEjecute.save({
                        enableSourcing: true,
                        ignoreMandatoryFields: true
                    });
                    return searchObj;
                }
            } catch (error) {
                log.error({title:'getInputData', details:error});
            }
        }

        /**
         * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
         * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
         * context.
         * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
         *     is provided automatically based on the results of the getInputData stage.
         * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
         *     function on the current key-value pair
         * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
         *     pair
         * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} mapContext.key - Key to be processed during the map stage
         * @param {string} mapContext.value - Value to be processed during the map stage
         * @since 2015.2
         */

        const map = (mapContext) => {
            try{
                var datos=JSON.parse(mapContext.value);
                mapContext.write({
                    key:mapContext.key,
                    value:datos
                });
            }catch(e){
                log.error({
                    title: "map",
                    details: e
                })
            }
        }

        /**
         * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
         * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
         * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
         *     provided automatically based on the results of the map stage.
         * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
         *     reduce function on the current group
         * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
         * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} reduceContext.key - Key to be processed during the reduce stage
         * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
         *     for processing
         * @since 2015.2
         */
        const reduce = (reduceContext) => {
            try {
                var data = JSON.parse(reduceContext.values);
                log.debug({title:'Data to delete', details:data});
                var deletedRecord = record.delete({
                    type: data.recordType,
                    id: data.id
                });
            } catch (error) {
                log.error({title:'reduce', details:error});
            }
        }


        /**
         * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
         * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
         * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
         * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
         *     script
         * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
         * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
         * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
         * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
         *     script
         * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
         * @param {Object} summaryContext.inputSummary - Statistics about the input stage
         * @param {Object} summaryContext.mapSummary - Statistics about the map stage
         * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
         * @since 2015.2
         */
        const summarize = (summaryContext) => {
            try {
                log.audit({title:'Fin del borrado', details:'Fin del borrado'});
                var parameter_deploy = runtime.getCurrentScript().getParameter({name: "custscript_fb_borrado_deplo"});
                var dataEjecute = record.load({
                    type: record.Type.SCRIPT_DEPLOYMENT,
                    id: parameter_deploy
                });
                dataEjecute.setValue({
                    fieldId: 'custscript_tkio_fin_ciclo',
                    value: false
                });
                var updScript = dataEjecute.save({
                    enableSourcing: true,
                    ignoreMandatoryFields: true
                });
                if (updScript) {
                    log.audit({title:'EJECUTANDO LA CREACION', details:'EJECUTANDO LA CREACION'});
                    var mrTask = task.create({taskType: task.TaskType.MAP_REDUCE});
                    mrTask.scriptId = 'customscript_tkio_guardado_combinaciones';
                    mrTask.deploymentId = 'customdeploy_tkio_guardado_combinaciones';
                    var mrTaskId = mrTask.submit();
                }
            } catch (error) {
                log.error({title:'summarize', details:error});
            }
        }

        return {getInputData, map, reduce, summarize}

    });
