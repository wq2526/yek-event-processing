/**
 * Created by wangqi on 2017/5/25.
 */
/**
 * This file is part of Qunee for HTML5.
 * Copyright (c) 2016 by qunee.com
 **/
$(function () {
    var graph = new Q.Graph('canvas');
    var model = graph.graphModel;
    var index = 0;
    var currentNode;
    
    var eventArray = new Array();
    var eventNum = 0;
    var eventProcessor = 0;

    $("#node_list").hide();
    $("#node_configuration").hide();

    $(".btn-group").children(".btn").click(function () {
        $(this).addClass("active");
        $(this).siblings().removeClass("active");
    });

    //custom interaction mode
    Q.Consts.INTERACTION_MODE_DELETE_NODE = 'interaction.mode.delete';
    Q.Defaults.registerInteractions(Q.Consts.INTERACTION_MODE_DELETE_NODE, [Q.PanInteraction, Q.WheelZoomInteraction, Q.SelectionInteraction, {
        onclick: function(evt, graph){
            graph.removeSelectionByInteraction(evt);
        }
    }]);

    $("#add").click(function(){
        var node = new Q.Node();
         index++;
         node.name = "node" + index;
         node.image = "node.svg";
         model.add(node);
    });

    $("#remove").click(function () {
        graph.interactionMode = Q.Consts.INTERACTION_MODE_DELETE_NODE;
    });

    $("#edge").click(function () {
        graph.interactionMode = Q.Consts.INTERACTION_MODE_CREATE_EDGE;
    });

    $("#default").click(function () {
        graph.interactionMode = Q.Consts.INTERACTION_MODE_DEFAULT;
    });

    $("#nodeList").click(function () {
        showList();
    });

    $("#return").click(function () {
        $("#node_list").hide();
        $("#node_configuration").hide();
        $("#graph_panel").show();
    });

    $("#add_event_type").click(function () {
    		event = new Object();
        event.id = eventNum;
        event.prop = 0;
        eventArray[eventNum] = event;
        var eventid = "event_type_input_" + eventNum;
        $(this).before(
            "<div class='form-group event-type'>"+
                "<label>事件名称</label>"+
                "<input type='text' class='form-control' id="+eventid+" placeholder='Event Type'>"+
                "<div id='event_props'>"+
        				"<div class='form-group' id='event_prop'>"+
        				"<label>事件属性</label>"+
        				"<div class='add-event-prop' id="+eventNum+"><a href='#'><i class='icon-plus-sign'></i></a></div>"+
        			"</div>"+
            "</div>");
        eventNum++;
        $(".add-event-prop").click(function () {
        		var eventId = $(this).attr("id");
        		var currentEvent = eventArray[eventId];
        		var propNum = currentEvent.prop++;
            var propid = eventId + "prop_name_input_" + propNum;
            var classid = eventId + "prop_class_input_" + propNum;
            $(this).before(
                "<div class='prop-inline'>"+
                    "<label>属性名称</label>"+
                    "<input type='text' class='form-control' id="+propid+" placeholder='Property Name'>"+
                    "<label>数据类型</label>"+
                    "<select class='form-control' id="+classid+">"+
                        "<option></option>"+
                        "<option>String</option>"+
                        "<option>int</option>"+
                        "<option>boolean</option>"+
                        "<option>float</option>"+
                        "<option>double</option>"+
                    "</select>"+
                "</div>");
        });
    });
    
    $("#add_event_processor").click(function () {
        var processorid = "event_process_input_" + eventProcessor++;
        $(this).before(
            "<div class='form-group event-processor'>"+
                "<label>操作</label>"+
                "<input type='text' class='form-control' id="+processorid+" placeholder='Event Processor'>"+
            "</div>");
    });

    $("#save").click(function () {
        currentNode.name = $("#node_name_input").val();
        currentNode.outEventType = $("#out_type").val();
        currentNode.num = $("#node_num").val();
        currentNode.processor = "[";
        currentNode.eventType = "[";
        for(var i=0;i<eventProcessor;i++){
            currentNode.processor = currentNode.processor + "\"" + $("#event_process_input_"+i).val() + "\",";
        }
        currentNode.processor = currentNode.processor + "]";
        
        for(var i=0;i<eventArray.length;i++){
        		currentNode.eventType = currentNode.eventType + "{" +
        		"\"event_type\":\"" + $("#event_type_input_"+i).val() + "\",";
        		var eventProps = "[";
        		var eventClasses = "[";
        		for(var j=0;j<eventArray[i].prop;j++){
        			eventProps = eventProps + "\"" + $("#"+i+"prop_name_input_"+j).val() + "\",";
        			eventClasses = eventClasses + "\"" + $("#"+i+"prop_class_input_"+j).val() + "\",";
        		}
        		eventProps = eventProps + "]";
        		eventClasses = eventClasses + "]";
        		currentNode.eventType = currentNode.eventType + 
        		"\"event_props\":" + eventProps + "," + 
        		"\"event_classes\":" + eventClasses + "},";
        }
        currentNode.eventType = currentNode.eventType + "]";
        
        showList();
    });

    $("#submit").click(function () {
    	
    	var data = "{\"nodes\":[";
    	model.forEachByTopoBreadthFirstSearch(function(node){
    		var children = "";
    		node.forEachOutEdge(function(edge){
    			children = children + edge.to + ","
    		});
    		
            data = data + "{" +
            		"\"id\":" + node.id + "," +
            		"\"name\":" + "\"" + node.name + "\"," +
            		"\"event_types\":" + node.eventType + "," +
            		"\"epl\":" + node.processor + "," +
            		"\"out_type\":" + "\"" + node.outEventType + "\"," +
            		"\"num\":" + node.num + "," +
            		"\"children\":[" + children + "]" +
            		"},"
        });
    	data = data + "]}";
    	
    	$.ajax({
			type : "POST",
			url : "/dag",
			data : {data:data},
			dataType : "json",
			success : function(data) {
				console.log("SUCCESS: ", data);
				display(data);
			},
			error : function(e) {
				console.log("ERROR: ", e);
				display(e);
			},
			done : function(e) {
				console.log("DONE");
			}
		});

    });

    function showList() {
        $("#graph_panel").hide();
        $("#node_configuration").hide();
        $("#node_table").empty();
        $("#node_table").append("<tr><th>Name</th><th>Event Type</th><th>Event Processor</th><th>Output Event Type</th><th>Operations</th></tr>");
        model.forEachByTopoBreadthFirstSearch(function(node){
            $("#node_table").append(
                "<tr>"+
                "<td>"+node.name+"</td>"+
                "<td>"+node.eventType+"</td>"+
                "<td>"+node.processor+"</td>"+
                "<td>"+node.outEventType+"</td>"+
                "<td id='"+node.id+"'><a href='#'>edit</a></td>"+
                "</tr>");
        });
        $("td:contains('edit')").click(function () {
            var nodeid = $(this).attr("id");
            currentNode = model.getById(nodeid);
            eventNum = 0;
            eventProcessor = 0;
            eventArray = new Array();
            $("#node_name_input").val(currentNode.name);
            $("#node_num").val("");
            $("#out_type").val("");
            $(".event-processor").remove();
            $(".event-type").remove();
            $("#node_list").hide();
            $("#node_configuration").show();
        });
        $("#node_list").show();
    }

});