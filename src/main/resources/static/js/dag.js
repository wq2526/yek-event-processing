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
    
    graph.onclick = function(evt) {
    	currentNode = graph.hitTest(evt);
    	$("#node_name_input").val(currentNode.name);
    	var eventNum = currentNode.eventTypes.length;
    	var processorNum = currentNode.processors.length;
    	for(var i=0;i<eventNum;i++){
    		var eventid = "event_type_input_" + (i+1);
    		var propNum = currentNode.eventTypes[i].props.length;
    		$("#add_event_type").before(
    	            "<div class='form-group event-type'>"+
    	                "<label>Event Type</label>"+
    	                "<input type='text' class='form-control' id="+eventid+" placeholder='Event Type'>"+
    	                "<div id='event_props'>"+
    	        				"<div class='form-group' id='event_prop'>"+
    	        				"<label>Event Property</label>"+
    	        				"<div class='add-event-prop' id="+(i+1)+"><a href='#'><i class='icon-plus-sign'></i></a></div>"+
    	        			"</div>"+
    	            "</div>");
    		for(var j=0;j<propNum;j++){
    			var propid = (i+1) + "prop_name_input_" + (j+1);
    			var classid = (i+1) + "prop_class_input_" + (j+1);
    			$(".add-event-prop#"(i+1)).before(
    	                "<div class='prop-inline'>"+
    	                    "<label>Property Name</label>"+
    	                    "<input type='text' class='form-control' id="+propid+" placeholder='Property Name'>"+
    	                    "<label>Property Type</label>"+
    	                    "<select class='form-control' id="+classid+">"+
    	                        "<option></option>"+
    	                        "<option>String</option>"+
    	                        "<option>int</option>"+
    	                        "<option>boolean</option>"+
    	                        "<option>float</option>"+
    	                        "<option>double</option>"+
    	                    "</select>"+
    	                "</div>");
    		}
    	}
    	$("#out_type").val(currentNode.outEventType);
    	$("#node_num").val(currentNode.num);
    }

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
         node.eventTypes = new Array();
         node.processors = new Array();
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

    $("#add_event_type").click(function () {
    	var	event = new Object();
        event.props = new Array();
        var eventNum = currentNode.eventTypes.push(event);
        var eventid = "event_type_input_" + eventNum;
        $(this).before(
            "<div class='form-group event-type'>"+
                "<label>Event Type</label>"+
                "<input type='text' class='form-control' id="+eventid+" placeholder='Event Type'>"+
                "<div id='event_props'>"+
        				"<div class='form-group' id='event_prop'>"+
        				"<label>Event Property</label>"+
        				"<div class='add-event-prop' id="+eventNum+"><a href='#'><i class='icon-plus-sign'></i></a></div>"+
        			"</div>"+
            "</div>");

        $(".add-event-prop").click(function () {
        	var eventId = $(this).attr("id");
        	var prop = new Object();
        	var propNum = currentNode.eventTypes[eventId-1].props.push(prop);
            var propid = eventId + "prop_name_input_" + propNum;
            var classid = eventId + "prop_class_input_" + propNum;
            $(this).before(
                "<div class='prop-inline'>"+
                    "<label>Property Name</label>"+
                    "<input type='text' class='form-control' id="+propid+" placeholder='Property Name'>"+
                    "<label>Property Type</label>"+
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
    	var processorNum = currentNode.processors.push("");
        var processorid = "event_process_input_" + processorNum;
        $(this).before(
            "<div class='form-group event-processor'>"+
                "<label>Event Processor</label>"+
                "<input type='text' class='form-control' id="+processorid+" placeholder='Event Processor'>"+
            "</div>");
    });

    $("#save").click(function () {
        currentNode.name = $("#node_name_input").val();
        currentNode.outEventType = $("#out_type").val();
        currentNode.num = $("#node_num").val();
        var eventNum = currentNode.eventTypes.length;
        var processorNum = currentNode.processor
        for(var i=0;i<processorNum;i++){
            currentNode.processors[i] = $("#event_process_input_"+(i+1)).val();
        }
        
        for(var i=0;i<eventNum;i++){
        		currentNode.eventTypes[i].name = $("#event_type_input_"+i).val();
        		var propNum = currentNode.eventTypes[i].props.length;
        		for(var j=0;j<propNum;j++){
        			currentNode.eventTypes[i].props[j].name = $("#"+(i+1)+"prop_name_input_"+(j+1)).val();
        			currentNode.eventTypes[i].props[j].className = $("#"+(i+1)+"prop_class_input_"+(j+1)).val();
        		}
        }      
        
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
            		"\"out_type\":" + node.outEventType + "," +
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

});