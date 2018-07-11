You can import this Grafana dashboard from the Grafana Dashboards->Import menu, or you can import it with Dashboard REST API using the following command issued on the Grafana host:

```
wget https://raw.githubusercontent.com/mapr-demos/predictive-maintenance/master/Grafana/IoT_dashboard.json
curl -X POST -H 'Content-Type: application/json;charset=UTF-8' --data "@./IoT_dashboard-POST.json" http://admin:admin@localhost:3000/api/dashboards/db
```
