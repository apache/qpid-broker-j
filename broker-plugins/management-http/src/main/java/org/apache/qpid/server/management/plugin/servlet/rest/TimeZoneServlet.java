package org.apache.qpid.server.management.plugin.servlet.rest;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class TimeZoneServlet extends AbstractServlet
{

    private static final String[] TIMEZONE_REGIONS = { "Africa", "America", "Antarctica", "Arctic", "Asia", "Atlantic", "Australia",
            "Europe", "Indian", "Pacific" };

    private final ObjectMapper _mapper;

    public TimeZoneServlet()
    {
        super();
        _mapper = new ObjectMapper();
        _mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    }

    @Override
    protected void doGetWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException
    {
        response.setContentType("application/json");
        writeObjectToResponse(getTimeZones(), request, response);

        response.setStatus(HttpServletResponse.SC_OK);
    }

    public List<TimeZoneDetails> getTimeZones()
    {
        List<TimeZoneDetails> timeZoneDetails = new ArrayList<TimeZoneDetails>();
        String[] ids = TimeZone.getAvailableIDs();
        long currentTime = System.currentTimeMillis();
        Date currentDate = new Date(currentTime);
        for (String id : ids)
        {
            int cityPos = id.indexOf("/");
            if (cityPos > 0 && cityPos < id.length() - 1)
            {
                String region = id.substring(0, cityPos);
                for (int i = 0; i < TIMEZONE_REGIONS.length; i++)
                {
                    if (region.equals(TIMEZONE_REGIONS[i]))
                    {
                        TimeZone tz = TimeZone.getTimeZone(id);
                        int offset = tz.getOffset(currentTime)/60000;
                        String city = id.substring(cityPos + 1).replace('_', ' ');
                        timeZoneDetails.add(new TimeZoneDetails(id, tz.getDisplayName(tz.inDaylightTime(currentDate), TimeZone.SHORT), offset, city, region));
                        break;
                    }
                }
            }
        }
        return timeZoneDetails;
    }

    public static class TimeZoneDetails
    {
        private String id;
        private String name;
        private int offset;
        private String city;
        private String region;

        public TimeZoneDetails(String id, String name, int offset, String city, String region)
        {
            super();
            this.id = id;
            this.name = name;
            this.offset = offset;
            this.city = city;
            this.region = region;
        }

        public String getId()
        {
            return id;
        }

        public String getName()
        {
            return name;
        }

        public int getOffset()
        {
            return offset;
        }

        public String getCity()
        {
            return city;
        }

        public String getRegion()
        {
            return region;
        }
    }
}
