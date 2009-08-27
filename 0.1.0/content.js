var cfg_showInherited=true;
var toggleInherited=function(){cfg_showInherited=!cfg_showInherited;
$.cookie("showInherited",cfg_showInherited);
updateInherited()
};
var updateInherited=function(){$("input.filter_inherited_cb").each(function(){this.checked=cfg_showInherited
});
if(cfg_showInherited){$("tr.isInherited").show()
}else{$("tr.isInherited").hide()
}};
$(document).ready(function(){parent.document.title=document.title;
cfg_showInherited=$.cookie("showInherited");
cfg_showInherited=(cfg_showInherited==true||cfg_showInherited=="true");
updateInherited();
$("div.apiCommentsDetails").hide()
});
var selectPackage=function(A){if(parent.navFrame){parent.navFrame.selectPackage(A)
}};
jQuery.cookie=function(B,I,L){if(typeof I!="undefined"){L=L||{};
if(I===null){I="";
L.expires=-1
}var E="";
if(L.expires&&(typeof L.expires=="number"||L.expires.toUTCString)){var F;
if(typeof L.expires=="number"){F=new Date();
F.setTime(F.getTime()+(L.expires*24*60*60*1000))
}else{F=L.expires
}E="; expires="+F.toUTCString()
}var K=L.path?"; path="+(L.path):"";
var G=L.domain?"; domain="+(L.domain):"";
var A=L.secure?"; secure":"";
document.cookie=[B,"=",encodeURIComponent(I),E,K,G,A].join("")
}else{var D=null;
if(document.cookie&&document.cookie!=""){var J=document.cookie.split(";");
for(var H=0;
H<J.length;
H++){var C=jQuery.trim(J[H]);
if(C.substring(0,B.length+1)==(B+"=")){D=decodeURIComponent(C.substring(B.length+1));
break
}}}return D
}};
