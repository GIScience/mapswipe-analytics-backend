<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor
  xmlns="http://www.opengis.net/sld"
  xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  version="1.1.0"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/StyledLayerDescriptor.xsd"
  xmlns:se="http://www.opengis.net/se">
  <NamedLayer>
    <Name>Tasks no_si</Name>
    <UserStyle>
    <Title>Tasks no_si</Title>
    <Abstract>The style for the Tasks</Abstract>
    <FeatureTypeStyle>
        <se:Rule>
          <se:Name>&lt;= 0.2</se:Name>
          <se:Description>
            <se:Title>&lt;= 0.2</se:Title>
          </se:Description>
          <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:PropertyIsLessThan>
              <ogc:PropertyName>no_si</ogc:PropertyName>
              <ogc:Literal>0.2</ogc:Literal>
            </ogc:PropertyIsLessThan>
          </ogc:Filter>
          <se:PolygonSymbolizer>
            <se:Fill>
              <se:SvgParameter name="fill">#ff0000</se:SvgParameter>
              <se:SvgParameter name="fill-opacity">0.5</se:SvgParameter>

            </se:Fill>
          </se:PolygonSymbolizer>
        </se:Rule>
        <se:Rule>
          <se:Name>0.2 - 0.4</se:Name>
          <se:Description>
            <se:Title>0.2 - 0.4</se:Title>
          </se:Description>
          <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:And>
              <ogc:PropertyIsGreaterThanOrEqualTo>
                <ogc:PropertyName>no_si</ogc:PropertyName>
                <ogc:Literal>0.2</ogc:Literal>
              </ogc:PropertyIsGreaterThanOrEqualTo>
              <ogc:PropertyIsLessThan>
                <ogc:PropertyName>no_si</ogc:PropertyName>
                <ogc:Literal>0.4</ogc:Literal>
              </ogc:PropertyIsLessThan>
            </ogc:And>
          </ogc:Filter>
          <se:PolygonSymbolizer>
            <se:Fill>
              <se:SvgParameter name="fill">#e8850c</se:SvgParameter>
              <se:SvgParameter name="fill-opacity">0.5</se:SvgParameter>

            </se:Fill>

          </se:PolygonSymbolizer>
        </se:Rule>
        <se:Rule>
          <se:Name>0.4 - 0.6</se:Name>
          <se:Description>
            <se:Title>0.4 - 0.6</se:Title>
          </se:Description>
          <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:And>
              <ogc:PropertyIsGreaterThanOrEqualTo>
                <ogc:PropertyName>no_si</ogc:PropertyName>
                <ogc:Literal>0.4</ogc:Literal>
              </ogc:PropertyIsGreaterThanOrEqualTo>
              <ogc:PropertyIsLessThan>
                <ogc:PropertyName>no_si</ogc:PropertyName>
                <ogc:Literal>0.6</ogc:Literal>
              </ogc:PropertyIsLessThan>
            </ogc:And>
          </ogc:Filter>
          <se:PolygonSymbolizer>
            <se:Fill>
              <se:SvgParameter name="fill">#ffff0d</se:SvgParameter>
              <se:SvgParameter name="fill-opacity">0.5</se:SvgParameter>

            </se:Fill>

          </se:PolygonSymbolizer>
        </se:Rule>
        <se:Rule>
          <se:Name>0.6 - 0.8</se:Name>
          <se:Description>
            <se:Title>0.6 - 0.8</se:Title>
          </se:Description>
          <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:And>
              <ogc:PropertyIsGreaterThanOrEqualTo>
                <ogc:PropertyName>no_si</ogc:PropertyName>
                <ogc:Literal>0.6</ogc:Literal>
              </ogc:PropertyIsGreaterThanOrEqualTo>
              <ogc:PropertyIsLessThan>
                <ogc:PropertyName>no_si</ogc:PropertyName>
                <ogc:Literal>0.8</ogc:Literal>
              </ogc:PropertyIsLessThan>
            </ogc:And>
          </ogc:Filter>
          <se:PolygonSymbolizer>
            <se:Fill>
              <se:SvgParameter name="fill">#8dff00</se:SvgParameter>
              <se:SvgParameter name="fill-opacity">0.5</se:SvgParameter>

            </se:Fill>

          </se:PolygonSymbolizer>
        </se:Rule>
        <se:Rule>
          <se:Name>
            0.8 - 1.0</se:Name>
          <se:Description>
            <se:Title>
              0.8 - 1.0</se:Title>
          </se:Description>
          <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:PropertyIsGreaterThanOrEqualTo>
              <ogc:PropertyName>no_si</ogc:PropertyName>
              <ogc:Literal>0.8</ogc:Literal>
            </ogc:PropertyIsGreaterThanOrEqualTo>
          </ogc:Filter>
          <se:PolygonSymbolizer>
            <se:Fill>
              <se:SvgParameter name="fill">#00ff00</se:SvgParameter>
              <se:SvgParameter name="fill-opacity">0.5</se:SvgParameter>
            </se:Fill>
          </se:PolygonSymbolizer>
        </se:Rule>
      </se:FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
