<mxGraphModel dx="1400" dy="900" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="1600" pageHeight="1000" math="0" shadow="0">
  <root>
    <mxCell id="0"/>
    <mxCell id="1" parent="0"/>

    <mxCell id="title" value="Strimzi 기반 CDC 플랫폼 전체 구조" style="text;html=1;strokeColor=none;fillColor=none;fontSize=24;fontStyle=1;align=center;" vertex="1" parent="1">
      <mxGeometry x="360" y="20" width="800" height="40" as="geometry"/>
    </mxCell>

    <mxCell id="aks" value="Azure AKS" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#f5f5f5;strokeColor=#1f4e79;fontStyle=1;fontSize=16;" vertex="1" parent="1">
      <mxGeometry x="40" y="90" width="1480" height="440" as="geometry"/>
    </mxCell>

    <mxCell id="operator" value="Strimzi Operator&lt;br&gt;&lt;br&gt;- Kafka Cluster 관리&lt;br&gt;- KafkaNodePool 관리&lt;br&gt;- KafkaConnect 관리&lt;br&gt;- KafkaConnector 관리" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#e2f0d9;strokeColor=#70ad47;fontSize=13;" vertex="1" parent="1">
      <mxGeometry x="80" y="180" width="250" height="180" as="geometry"/>
    </mxCell>

    <mxCell id="kafka" value="Kafka Cluster (KRaft)&lt;br&gt;&lt;br&gt;- Broker NodePool&lt;br&gt;- Controller NodePool&lt;br&gt;- CDC Topic&lt;br&gt;- Connect Internal Topic&lt;br&gt;- Schema History Topic" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#dae8fc;strokeColor=#4472c4;fontSize=13;" vertex="1" parent="1">
      <mxGeometry x="410" y="150" width="300" height="230" as="geometry"/>
    </mxCell>

    <mxCell id="connect" value="Kafka Connect&lt;br&gt;&lt;br&gt;- Debezium Oracle Source Connector 실행" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#fff2cc;strokeColor=#bf9000;fontSize=13;" vertex="1" parent="1">
      <mxGeometry x="790" y="180" width="240" height="130" as="geometry"/>
    </mxCell>

    <mxCell id="sink" value="PostgreSQL Sink / Consumer&lt;br&gt;&lt;br&gt;- Kafka CDC 이벤트를 PostgreSQL에 반영" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#eadcf8;strokeColor=#7030a0;fontSize=13;" vertex="1" parent="1">
      <mxGeometry x="1090" y="180" width="250" height="130" as="geometry"/>
    </mxCell>

    <mxCell id="akhq" value="AKHQ&lt;br&gt;&lt;br&gt;- Kafka 운영 및 모니터링 UI" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#fce4d6;strokeColor=#c55a11;fontSize=13;" vertex="1" parent="1">
      <mxGeometry x="1090" y="350" width="250" height="100" as="geometry"/>
    </mxCell>

    <mxCell id="e1" value="관리" style="endArrow=block;html=1;rounded=0;strokeWidth=2;" edge="1" parent="1" source="operator" target="kafka">
      <mxGeometry relative="1" as="geometry"/>
    </mxCell>

    <mxCell id="e2" value="Connector 실행" style="endArrow=block;html=1;rounded=0;strokeWidth=2;" edge="1" parent="1" source="connect" target="kafka">
      <mxGeometry relative="1" as="geometry"/>
    </mxCell>

    <mxCell id="e3" value="CDC 이벤트 소비" style="endArrow=block;html=1;rounded=0;strokeWidth=2;" edge="1" parent="1" source="kafka" target="sink">
      <mxGeometry relative="1" as="geometry"/>
    </mxCell>

    <mxCell id="e4" value="조회 / 모니터링" style="endArrow=block;html=1;rounded=0;strokeWidth=2;dashed=1;" edge="1" parent="1" source="akhq" target="kafka">
      <mxGeometry relative="1" as="geometry"/>
    </mxCell>

    <mxCell id="deploy" value="배포 구조 (GitOps)" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#f5f5f5;strokeColor=#7030a0;fontStyle=1;fontSize=16;" vertex="1" parent="1">
      <mxGeometry x="40" y="590" width="1480" height="300" as="geometry"/>
    </mxCell>

    <mxCell id="devops" value="DevOps Repository&lt;br&gt;&lt;br&gt;- Strimzi Helm Chart&lt;br&gt;- Kafka / KafkaNodePool / KafkaConnect 등 플랫폼 CR&lt;br&gt;- 기타 추가 CR&lt;br&gt;- Kustomize / Argo CD 배포 구성" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#d9eaf7;strokeColor=#1f4e79;fontSize=13;" vertex="1" parent="1">
      <mxGeometry x="100" y="680" width="360" height="160" as="geometry"/>
    </mxCell>

    <mxCell id="connectorRepo" value="Connector Repository&lt;br&gt;&lt;br&gt;- KafkaConnector / Debezium Connector 설정" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#e2f0d9;strokeColor=#70ad47;fontSize=13;" vertex="1" parent="1">
      <mxGeometry x="560" y="700" width="320" height="120" as="geometry"/>
    </mxCell>

    <mxCell id="argocd" value="Argo CD&lt;br&gt;&lt;br&gt;- DevOps Repository 동기화&lt;br&gt;- Connector Repository 동기화" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#fce4d6;strokeColor=#c55a11;fontSize=13;" vertex="1" parent="1">
      <mxGeometry x="1030" y="690" width="320" height="140" as="geometry"/>
    </mxCell>

    <mxCell id="e5" value="Git Sync" style="endArrow=block;html=1;rounded=0;strokeWidth=2;" edge="1" parent="1" source="devops" target="argocd">
      <mxGeometry relative="1" as="geometry"/>
    </mxCell>

    <mxCell id="e6" value="Git Sync" style="endArrow=block;html=1;rounded=0;strokeWidth=2;" edge="1" parent="1" source="connectorRepo" target="argocd">
      <mxGeometry relative="1" as="geometry"/>
    </mxCell>

    <mxCell id="e7" value="Deploy / Sync" style="endArrow=block;html=1;rounded=0;strokeWidth=2;dashed=1;" edge="1" parent="1" source="argocd" target="aks">
      <mxGeometry relative="1" as="geometry"/>
    </mxCell>

  </root>
</mxGraphModel>