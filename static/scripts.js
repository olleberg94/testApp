var request = new XMLHttpRequest();
var result = {};
var message;
var trigger;
var txt;
var city;
var Email;

request.responseType = 'json';

$(document).ready(function(){
  $('.btnload-api').on('click', function() {
    var requestURL = $('#requestURL').val();
    var dataSet = $('#dataSet').val();
    var tableName = $('#tableName').val();
    var project = $('#gmailID').val();
    var data = {requestURL: requestURL, dataSet: dataSet, tableName: tableName, project: project}
    req = $.ajax({
      url: '/loadapi',
      type: 'POST',
      data: data
    });
     req.done(function(data) {
       $('.tableDiv').html(data);
       document.getElementById("loading").style.display = "None";
     });

  })
})

function getResponse() {
  result = request.response;
  message = JSON.stringify(result);
  console.log(result);
  generateTable(message);
}
function startLoad() {
   document.getElementById("loading").style.display = "block";
}

function generateTable(response) {
  weatherObj = JSON.parse(response);
  if (weatherObj != null) {
  var temp = weatherObj.temp;
  var weatherDesc = weatherObj.weather.description;
  var windSpeed = weatherObj.wind.speed;
  var time = weatherObj.dt_txt;
  document.getElementById("resultDiv").innerHTML =
  "<table class=\"weatherTable\"><tr><td>Temperature</td><td>"+temp+"</td></tr><tr><td>Description</td><td>"+weatherDesc+"</td></tr><tr><td>Wind Speed</td><td>"+windSpeed+"</td></tr><tr><td>Time</td><td>"+time+"</td></tr></table>";
  }
  if (weatherObj == null){
    document.getElementById("resultDiv").innerHTML = "<table class=\"weatherTable\"><tr><td>City not found</td></tr></table>"
  }
}


function resetRequest() {
  document.getElementById("resultDiv").innerHTML = "<table class=\"weatherTable\"><tr><td>RESET</td></tr></table>";
}

function logon() {
  Email = document.getElementById("gmailID").value;
  document.getElementById("emailBody").style.display = "none";
  document.getElementById("weatherBody").style.display = "block";
  document.getElementById("welcomeMsg").innerHTML = "Load API to database in "+Email;
}
function logout() {
  Email = null;
  document.getElementById("emailBody").style.display = "block";
  document.getElementById("weatherBody").style.display = "none";
  document.getElementById("welcomeMsg").innerHTML = null;
}